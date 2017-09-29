#!/usr/bin/perl -w

# DataCompare.pl - script and manual for comparing data in multiple 
#		   schemas/tables across multiple databases
# Version 1.2.0
# (C) 2016 - Radoslaw Karas <rj.cork@gmail.com>
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA



use strict;
#use Cwd;
use File::Basename;
#use lib (dirname(Cwd::abs_path($0)).'/lib');
use Lib::DataCompare;
use Lib::Database;
use Lib::Logger qw(PrintMsg ERROR WARNING DEBUG DEBUG1 DEBUG2 DEBUG3 INFO);
use Storable;
use Data::Dumper;
use Getopt::Long;
use Pod::Usage;
use POSIX; #qw(strftime :sys_wait_h :signal_h);
use IO::Select;
use IO::Handle;
use IO::Pipe;

my %CONFIG = (
		'continue_only' => 0,
		'parallelism' => 4,
		'sql_parallelism' => 1,
		);
my %CHILDREN;
my $SKIP_PARTS_ROW_LIMIT = 10000000;# Lib::Database::RESULT_BATCH_SIZE;

# lib ======================================================================================================

sub ReadKey {
        my ($term, $oterm, $echo, $noecho );
        $term     = POSIX::Termios->new();
        $term->getattr(fileno(STDIN));
        $oterm     = $term->getlflag();
        $echo     = ECHO | ECHOK | ICANON;
        $term->setlflag($oterm & ~$echo);
        $term->setattr(fileno(STDIN), TCSANOW);
        sysread(STDIN, $_, 1);
        $term->setlflag($oterm);
        $term->setattr(fileno(STDIN), TCSANOW);
        return $_;
}

sub GetPass {
        my $prompt = shift;
        my $i='';
        my $pass;
        print STDERR $prompt;
        while (($i=ReadKey()) ne "\n") {
                $pass.=$i;
        }
        chomp($pass) if (defined($pass));
        print STDERR "\n";
        return $pass;
}

#remove password from process arguments
sub SetProcessName {
	my @args = @_;
        my $new_name = (caller)[1];
	
	if (defined($CHILDREN{'child'})) {
		$new_name = '[WORKER] '.$new_name;
	}

        while (my $a = shift @args) {
                if ($a eq '--db' || $a eq '-d' || $a eq '--basedb' || $a eq '--primary') {
                        my $b = shift @args; #skip
			if (!defined($CHILDREN{'child'}) && $b =~ /(?:([\w\d]+)=)?([\w\d]+)(?:\/([\w\d]+))?\@([\w\d\.\-]+)(?::(\d+))?\/([\w\d]+)/) {
                           #alias, user, pass, host, port, service
			   	$new_name .= " $a ";
			   	$new_name .= "$1=" if (defined($1));
			   	$new_name .= "$2@" if (defined($2));
				$new_name .= "$4";
				$new_name .= ":$5" if (defined($5));
				$new_name .= "/$6";
			}

                } elsif ($a eq '--auxuser') {
                        $a = shift @args;
                        $a =~ s/(\S+?)\/?/$1/;
                        $new_name .= ' --auxuser '.$a;
                } else {
                        $new_name .= " $a";
                }
        }
	$0 = $new_name;
}
  
#=== Time verification functions ===
sub ResolveDay {
	my $d = shift;
	my @n = qw( Sunday Monday Tuesday Wednesday Thursday Friday Saturday );

	if (length($d)<2) {
		PrintMsg ERROR, "ResolveDay(): $d too short to recognize the day";
		exit 1;
	}

	for (my $i=0;$i<7;$i++) {
		return $i if ($n[$i] =~ /^$d/i);
	}
	
	PrintMsg ERROR, "ResolveDay(): $d what day is that?";
	exit 1;
}

sub ResolveHour {
	my $t = shift;

	if ($t =~/^(\d+)(:(\d+))?$/) {
		my $h = $1;
		my $m = (defined ($3)) ? $3 : 0;
		if ($h<0 || $h>23) {
			PrintMsg ERROR, "ResolveHour(): Invalid hour: $h";
			exit 1;
		}

		if ($m<0 || $m>59) {
			PrintMsg ERROR, "ResolveHour(): Invalid minute: $m";
			exit 1;
		}

		return $h*60+$m;
	} 

	PrintMsg ERROR, "ResolveHour(): Invalid time given: $t";
	exit 1;
}
# returns 1 if current time matches any given time range
sub VerifyTime { 
	my @ranges = @_;
	my @lt = localtime(time);
	my $day_match;
	my $hour_match;

	return 1 if (scalar(@ranges) == 0);

	#  0    1    2     3     4    5     6     7     8
        #($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)
	foreach my $r (@ranges) {

		$day_match = $hour_match = 0;

		if ($r =~ /^\s*([a-z,\s\-]+)?\s*([0-9,:\-\s]+)?\s*$/i) { #get new time range
			my $d = $1;
			my $h = $2;
			if (defined $d) { #check days part
				foreach my $i (split(',',$d)) {
					my ($r1,$r2);
					PrintMsg(DEBUG2, "VerifyTime(): $i\n");
					if ($i =~ /(\w+)\-(\w+)/) {
						$r1 = ResolveDay($1);
						$r2 = ResolveDay($2);
						PrintMsg(DEBUG2, "VerifyTime(): Day: $r1 - $r2\n");
					} elsif ($i =~ /(\w+)/) {
						$r1 = $r2 = ResolveDay($1);
						PrintMsg(DEBUG2, "VerifyTime(): Day: $r1\n");
					} else {
						PrintMsg(ERROR, "VerifyTime(): Error parsing days\n");
						exit 1;
					}
					if ( $r1 > $r2 ) { # 1-0 (Mo-Sun); 5-2 (Fri-Tue)
						if ( ($r2 >= $lt[6]) || ($r1 <= $lt[6]) ) {
							$day_match = 10;
							last;
						}
					} else { #1-5 (Mo-Fri); 0-3 (Sun-We)
						if ( $r1 <= $lt[6] && $lt[6] <= $r2) { #day match - check hours now
							$day_match = 10;
							last;
						}
					}
				}
			} else {
				$day_match = -10; #day range is not given - implicytly assume that day matched
			}

			if (defined $h) { #hours defined 
				foreach my $j (split(',',$h)) { #19:30-21,22,23-5
					my ($t1,$t2,$t3,$t4);
                                        PrintMsg(DEBUG2, "VerifyTime(): $j\n");

					if ($j =~ /^(\d+(?::\d+)?)\-(\d+(?::\d+)?)$/) {
						($t3,$t4) = ($1,$2);
						$t1 = ResolveHour($t3);
						$t2 = ResolveHour($t4);
					} elsif ($j =~ /^(\d+)(?::(\d+))?$/) {
						($t3,$t4) = ($1,$2);
						if (defined($t4)) {
							$t1 = $t2 = ResolveHour("$t3:$t4");
						} else {
							$t1 = ResolveHour("$t3:00");
							$t2 = ResolveHour("$t3:59");
						}
					} else {
						PrintMsg(ERROR,  "VerifyTime(): Error parsing time: $j\n");
					}

					my $t = $lt[2]*60+$lt[1];
					PrintMsg(DEBUG3,  "VerifyTime(): time ",int($t/60),":",$t%60,
								", check in <",int($t1/60),":",$t1%60,
								",",int($t2/60),":",$t2%60,">\n");

					if ($t1 > $t2) { # 22-5
					      if ( ($t1 <= $t) || ($t2 >= $t) ) {
						 PrintMsg(DEBUG3,  "VerifyTime(): hour match\n");
						 $hour_match = 10;
						 last;
					      }
					} else { # 9-12 or 9-9
					      if ( ($t1 <= $t) && ($t <= $t2) ) {
						 PrintMsg(DEBUG3,  "VerifyTime(): hour match\n");
						 $hour_match = 10;
						 last;
					      }
					}
				}
			} else {
				$hour_match = -10; #hour is not given - implicytly assume that hour matched
			}

			if ($hour_match == -10 && $day_match == -10) {
				PrintMsg(ERROR,  "VerifyTime(): Invalid time range: $r\n");
			} elsif ($hour_match == 0 || $day_match == 0) {
				PrintMsg(DEBUG1,  "VerifyTime(): didn't match $r\n");
			} else {
				PrintMsg(DEBUG2, 'VerifyTime(): Current ', strftime("%A %H:%M",@lt), " matched range: $r\n");
				return 1;
			}

		} else {
			PrintMsg(ERROR, "VerifyTime(): --range wrong format\n");
			exit 1;
		}
	}
	return 0;
}

#=== State saving functions ===
sub LoadStateFile {
        my $f = shift;
	my $dbs = shift;
	my %state_file;

        return if(!defined $f);

        if ( -f $f && -R $f ) { 
                %state_file = %{retrieve($f)} or do { PrintMsg(ERROR, "LoadStateFile(): Error reading state file $f: $!\n"); exit 1; }
        } else {
		if (-e $f) { # and the file exists
	                PrintMsg(ERROR, "LoadStateFile(): File $f is not a regular or readable file.\n");
			exit 1;
		} 
		return (undef, undef); #new file needs to be created
        }

	if (!defined($state_file{OBJECTS})) {
		PrintMsg(ERROR, "LoadStateFile(): invalid state file.\n");
		exit 1;
	}

	foreach my $db (keys %{$dbs}) {
		if (!defined($state_file{OBJECTS}->{$db})) {
			PrintMsg(ERROR, "LoadStateFile(): Invalid state file. Missing tables for $db.\n");
			exit 1;
		}
	}
	
	return ($state_file{TABLES}, $state_file{REPORT});
}

# filename, tables hash = null if remove file
sub SaveStateFile {
	my %sav;

        my $f = shift;
	$sav{'OBJECTS'} = shift;
	$sav{'RESULT'} = shift;

        return if(!defined $f);

	PrintMsg(DEBUG, "SaveStateFile(): Saving state in $f\n");
        
        if ( ! -e $f || ( -f $f && -W $f ) ) { 
		if ($sav{'OBJECTS'}) { # 2nd arg is undef if there is no tables left to process
	                store \%sav, $f or do { PrintMsg(ERROR,  "SaveStateFile(): Error writing current state to file $f: $!\n"); exit 1; }
		} else { #nothing left to do, we dont need state file any more
			unlink $f or PrintMsg(ERROR,  "SaveStateFile(): Cannot remove state file $f: $!\n");
		}
        } else {
                PrintMsg(ERROR, "SaveStateFile(): File $f is not a regular or writeable file.\n");
		exit 1;
        }
}
# ============================

sub CreatePidfile {
	my $pid_file = shift;
	my $PF;

        return if(!defined $pid_file);
        
        if ( -e $pid_file ) { #exists?
		if ( -f $pid_file && -W $pid_file ) { #plain file, writable ?
			open PF, "$pid_file" or do { PrintMsg(ERROR, "CreatePidfile(): Cant open $pid_file for reading\n"); exit 1; };       
                        my $pid = <PF>;
			close PF;

			if ( ! -e "/proc/cpuinfo" ) {
				PrintMsg(ERROR, "CreatePidfile(): /proc filesystem not mounted.\n"); 
                                exit 1;
			}
			if ( -e "/proc/$pid") {
				PrintMsg(ERROR, "CreatePidfile(): $pid is still running. Exiting.\n"); 
				exit 1;
			}
		} else {
			PrintMsg(ERROR, "CreatePidfile(): $pid_file is not writable or is not a plain file. Exiting.\n"); 
                        exit 1;
		}
        } 

	#there is no $pid_file, we can (re)create it
	open PF, ">$pid_file" or do { PrintMsg(ERROR, "CreatePidfile(): Cant open $pid_file for writing\n"); exit 1; };
	print PF $$;
	close PF;
}

#output is an array of 
sub GetObjectList {
	my $dbh = shift;
	my $schema = shift;
	my $table = shift;

	my %list;
	my $sql = "select t.owner,t.table_name,p.partition_name,p.high_value,p.partition_position,t.num_rows t_num_rows,p.num_rows p_num_rows from all_tables t left join all_tab_partitions p on t.owner=p.table_owner and t.table_name=p.table_name where ";

	$dbh->{LongReadLen} = 1000; # high_value is long
#???		#when choosing partition using high value substract 1 second or less - 1/86400
#???		#select count(*) from DATA_OWNER.TAB partition for (TO_DATE(' 2016-11-17 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')-1/86400);
#???		#select count(*) from DATA_OWNER.TAB partition (SYS_P29878);

	$schema = uc($schema);
	PrintMsg(DEBUG3, "GetObjectList(): $schema\n");

	if ($table and $table =~ m/[\?\*%]/) { #are there any wild cards?
		$table = uc($table);
                $table =~ s/\_/\\_/g; #escape underscores -> data_owner to data\_owner
                $table =~ s/\*/%/g; #change * to %
                $table =~ s/\?/_/g; #change ? to _
                $sql .= " t.table_name like '$table' escape '\\' and ";
        } elsif ($table) {
		$table = uc($table);
                $sql .= " t.table_name = '$table' and ";
        }

	if ($schema =~ m/[\?\*%]/) { #are there any wild cards?
		$schema =~ s/\_/\\_/g; #escape underscores -> data_owner to data\_owner
		$schema =~ s/\*/%/g; #change * to %
		$schema =~ s/\?/_/g; #change ? to _
		$sql .= " t.owner like '$schema' escape '\\' order by 1,2,3";
	} else {
		$sql .= " t.owner = '$schema' order by 1,2,3";
	}

	PrintMsg DEBUG3, "GetObjectList(): $sql\n";

	my %slice;
	my $r = $dbh->selectall_arrayref($sql, { Slice => \%slice } );
	my $pk_cols = []; #table ref
	my $processed_p = '';

	foreach my $row (@{$r}) {
		my %cols;
		my $p = $row->{OWNER}.'.'.$row->{TABLE_NAME};
		my %obj = ('owner' => $row->{OWNER}, 'table' => $row->{TABLE_NAME}); #for Lib::Database::GetPrimaryKey

		if ($p ne $processed_p) { #if table name is changed
			$processed_p = $p; #store it
			$pk_columns = []; #reset pk info 
		}
		# - if not defined ($row->{PARTITION_NAME}) then it is table without partitions
		#	then 	#table w/o partitions
		#		if defined $row->{T_NUM_ROWS} and $row->{T_NUM_ROWS} > $SKIP_PARTS_ROW_LIMIT
		#			#statistics show that it is too big to run in one pass
		#			@pk=get_pk
		#			do virtual partitioning by sample
		#		else #no stats or table is small
		#			add to list as normal table: $list{$p} = ''
		#	else #with partitions
		#		check high_value
		#		if defined $row->{P_NUM_ROWS} and $row->{P_NUM_ROWS} > $SKIP_PARTS_ROW_LIMIT
		#			#statistics show that this partition is too big to run in one pass
		#			do virtual subpartitioning by sample
		#		else
		#			add to list
		
		if (not defined ($row->{PARTITION_NAME})) {
			$list{$p} = '';
		} else { #partitions are here
			if (defined($row->{HIGH_VALUE})) {
				$p .= '.'.$row->{PARTITION_POSITION}; #high_value as partition name is poor idea

				#partition for (TIMESTAMP' 2017-09-27 04:00:00' - INTERVAL '1' SECOND)
				$list{$p} = 'partition for ('.$row->{HIGH_VALUE}." - INTERVAL '1' SECOND)";
			} else {
				$p .= '.'.$row->{PARTITION_NAME};
				$list{$p} = 'partition ('.$row->{PARTITION_NAME}.')';
			}

			if (defined($row->{P_NUM_ROWS}) && $row->{P_NUM_ROWS} > $SKIP_PARTS_ROW_LIMIT) {
			#partition too big, need to split it

				if (scalar(@{$pk_cols}) == 0) {
					if (Lib::Database::GetPrimaryKey(\%cols, $dbh, %obj, 'GetObjectList',Lib::Database::PK_DONT_CHECK) < 0) {
						exit 1;		
					}
					@{$pk_cols} = sort { $cols{$a}->{CPOSITON} <=> $cols{$b}->{CPOSITON} } grep {defined $cols{$_}->{CPOSITON}} keys %cols;
				}
				my $object_rowcount = $row->{P_NUM_ROWS}; 
				my $vpart_count = int($object_rowcount / $SKIP_PARTS_ROW_LIMIT + 1) #ceil($object_rowcount / $SKIP_PARTS_ROW_LIMIT)
				my $new_vparts = Lib::Database::GetVirtualPartitions($dbh,
											$pk_columns, 
											$vpart_count/$object_rowcount, #estimated sample size
											$row->{OWNER},
											$row->{TABLE_NAME},
											$list{$p});

				for (my $vp = 0; $vp < scalar(@{$new_vparts}); $vp++ ) {
					$list{$p.'.vp'.$vp} = $new_vparts->[$vp];
				}

			} 
		}

#		PrintMsg(DEBUG2, "GetObjectList(): table: $p ",($list{$p})?",part high val: $list{$p} \n":"\n");
	}

	return \%list;
}

sub GetObjects {
	my $dbs = shift;
	my $objects = shift;
	my %objects_all;

	foreach my $db (keys %{$dbs}) {
		my $dbh = Lib::Database::Connect($dbs->{$db}, 'GetObjects');

		if (not defined($dbh)) {
			PrintMsg(ERROR, "Connection failed to $db->{NAME}\n");
			exit 1;
		}

		foreach my $o (@{$objects}) { #schema[.table[.partition]]
			my $h_ref;

				# $1 (obligatory) - schema, $2 (optional) - table, TODO: $3 (optional) - partition
			if ($o =~ /^([\w\d\?\*%]+)(?:\.([\w\d\$\?\*%]+))?$/) { 
				$h_ref = GetObjectList($dbh, $1, $2);
			} else {
				PrintMsg(ERROR, "Incorrect object name for comparison: $o. \n");
				exit 1;
			}

			map {$objects_all{$db}->{$_} = $h_ref->{$_}} keys %{$h_ref};
		}
	}

	return \%objects_all;
}
# ======================================================================================================

sub GetParams {
	my @dbs;
	my $basedb;
	my %databases;

	my ($auxuser, $auxpass);
	my ($restart, $debug_lvl, $dry_run, $state_file, $max_load, $pid_file, $log_path, $log_dir);

	my @objects;
	my @cmp_columns;
	my @cmp_sha;
	my @mappings;
	my @excludes;
	my @time_frames;

	my $help = 0;
	my $i = 0;

	GetOptions ('db|d=s' => \@dbs,
	    'basedb|primary=s' => \$basedb,
	    'auxuser=s' => \$auxuser,
	    'schema|table|s=s' => \@objects,
	    'comparecolumn|comparecol|c=s' => \@cmp_columns,
	    'comparesha|comparesum=s' => \@cmp_sha,
	    'restart' => \$restart, #$CONTINUE_ONLY,
            'verbose|v+' => \$debug_lvl,
	    'parallelism|p=i' => \$CONFIG{parallelism},
	    'sqlparallelism=i' => \$CONFIG{sql_parallelism},
	    'remap|m=s' => \@mappings,
	    'exclude|e=s' => \@excludes,
	    'timeframe|t=s' => \@time_frames,
	    'dry|test' => \$dry_run,
	    'statefile|state|f=s' => \$state_file,
	    'outputfile|logfile|log|o|l=s' => \$log_path,
	    'logdir=s' => \$log_dir,
	    'maxload=i' => \$max_load,
	    'pid|pidfile=s' => \$pid_file,
	    'help|h' => \$help) or $help=100;

	if ($help) {
                #pod2usage(1);
                pod2usage( -verbose => 2, -exitval => 1, -output  => \*STDOUT);
                exit 0;
        }

	
	if ($CONFIG{parallelism} > 16 || $CONFIG{parallelism} < 1) {
		PrintMsg(ERROR, "GetParams(): --parallelism outside valid range 1 to 16.\n");
		exit 1;
	}

	if ($CONFIG{sql_parallelism} > 24 || $CONFIG{sql_parallelism}  < 1) {
		PrintMsg(ERROR, "GetParams(): --sqlparallelism outside valid range 1 to 24.\n");
		exit 1;
	}

	if ($auxuser) {
		if ($auxuser =~ /([\w\d]+)(?:\/([\w\d]+))?/) {
			($auxuser,$auxpass) = ($1,$2);
			$auxpass = DataCompare::GetPass("Password for $auxuser:") if (!$auxpass);
		} else {
			PrintMsg(ERROR, "GetParams(): invalid --auxuser. Should be --auxuser=someuser/somepassword\n");
			exit 1;
		}
	}		

	if ($pid_file) {
		$CONFIG{'pid_file'} = $pid_file;
	}

	if ($state_file) {
		$CONFIG{'state_file'} = $state_file;
	}

	if (VerifyTime(@time_frames) == 0) {
		PrintMsg(ERROR, "GetParams(): I'm outside available time slots. Check --timeframe|-t parameter.\n");
		exit 1;
	}

	foreach my $d ($basedb, @dbs) {
		my $dbname;
		my %db;
		
		next if (not defined $d);

		if ($d =~ /(?:([\w\d]+)=)?([\w\d]+)(?:\/([\w\d]+))?\@([\w\d\.\-]+)(?::(\d+))?\/([\w\d]+)/) {
			   #alias, user, pass, host, port, service
			($db{'alias'}, $db{'user'}, $db{'pass'}, $db{'host'}, $db{'port'}, $db{'service'}) = ($1, $2, $3, $4, $5, $6);
		} else {
			PrintMsg(ERROR, "GetParams(): Connect string $d doesn't match the right format: [alias=]user[/pass]\@host[:port]/service\n");
			exit 1;
		}

		if ($auxuser) {
			$db{'auxuser'} = $auxuser;
			$db{'auxpass'} = $auxpass;
		}		

		if (not defined($db{'port'})) {
	                $db{'port'} = 1521;
	        }

		if (not defined($db{'pass'})) {
			$db{'pass'} = GetPass("Password for $d:");
		}

		if (not defined($db{'alias'})) { #is there name-alias given for this connection string?
			$db{'alias'} = "DB$i";
		} else {
			$db{'alias'} = uc($db{'alias'});
		}
		$dbname = $db{'alias'};
		$basedb = $dbname if (not defined($basedb)); #primary

		if (defined($databases{$dbname})) { # check if name (alias) is already taken
			print "ERROR: alias $dbname is already taken. Please choose another.\n";
			exit 1;
		}

		$databases{$dbname} = \%db;
		$databases{$dbname}->{'NAME'} = $dbname;

		$i++;
	}

	if (scalar(keys %databases) < 2) {
		PrintMsg ERROR, "GetParams(): Provide two or more database connection strings. --db=user\@host1/service1 --db=user\@host2/service2\n";
		exit 1;
	}

	if (scalar(@objects) == 0) {
		PrintMsg ERROR, "GetParams(): Schema name or table name is needed. --schema=schema1\n";
		exit 1;
	} 

#	foreach my $m (keys %maps) {
#		if (not defined($PROGRESS{$m})) {
#			print "ERROR: Invalid mapping parameter. There is no $m defined.\n";
#			exit 1;
#		} else { #for now we accept only one table, in future there will be some
#			# oldschema.oldtable:newschema.newtable
#			$MAPPINGS{$m} = uc($maps{$m});
#			# at the moment oldschema.oldtable is always the same: $TABLENAMES[0]
#			# so map is just newschema.newtable
#		}
#	}

#	foreach my $mp (keys %mapparts) {
#		my @pmaps= split(/,/,uc($mapparts{$mp}));
#		$PARTMAPPINGS{$mp} = &share([]);
#		@{$PARTMAPPINGS{$mp}} = @pmaps;
#		PrintMsg "Partition mappings: $mp -> ",join(':',@{$PARTMAPPINGS{$mp}}),"\n" if ($DEBUG > 2);
#	}
	
#	$CMP_COLUMN = uc($CMP_COLUMN) if (defined($CMP_COLUMN));
	return (\%databases,\@objects);
}

sub Terminate {
	PrintMsg(ERROR, "Terminate(): ", POSIX::strftime('%y/%m/%d %H:%M:%S', localtime), "\n");

	if (defined($CHILDREN{'child'})) { #is it child?

		PrintMsg(INFO, "Child process $$ - exiting.\n");

	} else { #or parent?

		ShutdownWorkers();
		PrintMsg(INFO, "Parent process $$ - exiting.\n");

        	if ( $CONFIG{'pid_file'} && -e $CONFIG{'pid_file'} &&  -f $CONFIG{'pid_file'} && -W $CONFIG{'pid_file'} ) { 
			PrintMsg(DEBUG, "Removing $CONFIG{pid_file}.\n");
			unlink $CONFIG{pid_file};
		}

	}

	exit 1;
}

sub main {
	my $report;
	my $databases;
	my $list;
	
	$| = 1; #no buffering on stdout

	$SIG{INT} = \&Terminate;
	$SIG{TERM} = \&Terminate;

	($databases, $list) = GetParams();

	SetProcessName(@ARGV);

	if ($CONFIG{'state_file'}) {  #for now we are reprocessing old list
		($list, $report) = LoadStateFile($CONFIG{'state_file'}, $databases);
	}


	CreatePidfile($CONFIG{'pid_file'});

	if (not defined($report)) { #this is fresh start, no tables in statefile or no statefile
		if ($CONFIG{'continue_only'}) {
			PrintMsg ERROR, "Stopping, This is fresh start and --continueonly flag enabled. (",POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),")\n";
			exit 0;
		}
		$report = {};
		$list = GetObjects($databases, $list);
	}
print Dumper($list);
#	CompareObjects($list, $report, $databases);

}


main;


# --range "Mo-We 20:00-6:00"
# --schema schema[.tablename] #if %/? then like 
# --exclude [db2=]schema[.tablename][.column] #if %/? then like %/_ LIKE '%A\_B%' ESCAPE '\'; ; column without wildcards
# --comparecol [schema.][tablename.]column --comparecol [schema2.][tablename2.]column2
# --comparekey [schema1.]t?ble%1 -> relates to BASEDB/PRIMARY
# --remap [db2=]schema1[.tablename1][.part1]:schema2[.tablename2][.part2] <- without multiple partition mapping

__END__

=head1 NAME

=over 4
 
 DbCompare.pl - Script for comparing data in schemas across many databases.

=back

=head1 SYNOPSIS

DbCompare.pl --db [dbalias=]user[/pass]@host1[:port]/service --db [dbalias2=]user[/pass]@host2[:port]/service --schema schema [--comparecol [schema.][table.]column] [--comparekey [schema.]table] [--parallel NUMBER] [--statefile file.state] [--logfile log.txt] [--pidfile file.pid] [--maxload NUMBER] [--remap dbalias=schema1[.table1]:schema2[.table2]] [--range "TIME SPECIFICATION"] [--exclude schema[.table]] [--auxuser user[/pass]] [--test] [-v] [--help|-h]

=head1 DESCRIPTION

DbCompare.pl connects to multiple databases and compares tables in specified schemas. Comparision may be performed using values in specified column or record's SHA1 hash sum, which is calculated for all columns except PK columns.

=head2 Options

=over 4

=item -d, --db, --basedb [dbalias=]user[/pass]@host[:port]/service

Provides user credentials and connect string to instance providing data for comparision. You can connect to multiple databases. You need at least two connections defined. You can assign names (dbalias) for those connections so you can refer to them in mapping options. The first database connection you will define is base database. The list of tables is taken from there and all other databases are compared to the base one. You can use --basedb parameter instead of --db parameter to specify explicitly which is the primary connection.

=item -s, --schema, --table SCHEMA[.TABLE]

Compares tables in given schema. Tables are taken from connection defined as basedb. If not defined it will be first connection. You can use %, * or ? as wildcards. You can use multiple --schema options.

=item -m, --remap [db2=]schema1[.tablename1]:schema2[.tablename2]

For given connection referred by dbalias set different schema name and/or different table name. You can use %, * or ? as wildcards. For mapping all tables starting with INACTV in DATA_OWNER schema to tables starting with INACTIVE in DATA_USER schema in dbalias2 database/connection use --remap dbalias2=DATA_OWNER.INACTV%:DATA_USER.INACTIVE% You can use multiple --remap options. 

=item -e, --exclude [db2=]schema[.tablename] 

Skips table or all tables in given schema during table list generation. You can use %, * or ? as wildcards. You can use multiple --exclude options. 

=item -e, --exclude schema.tablename.columnname 

In case SHA1 is used for data comparison, you can choose which column to skip during hash calculation for specified table. You can use %, * or ? as wildcards only in schema or table names. You can use multiple --exclude options. 

=item -c, --comparecol, --comparecolumn [schema.][tablename.]column 

Compares rows by given column. It should be timestamp column and it should be updated each time a DML modifies the row. It is much faster than comparing whole row using SHA1 hash. If only column is given, then all tables in all schemas will be compared using this column. If column with table name is given then the column will be used only for spectified table in all schemas provided in --schema parameter. You can use %, * or ? as wildcards. You can use multiple --compare options.

=item --comparekey [schema.]table

Compares rows using PK or unique key. It is the fastest way for comparision as the database is performing fast full index scan only. If only table name is given then the PK comparison will be used only for spectified table in all schemas provided in --schema parameter. You can use %, * or ? as wildcards. You can use multiple --comparekey options. 

=item -p, --parallel NUMBER

Choose level of parallelism for query calculating SHA1 sums. Defaults to 4.

=item -r, --range "Mo-We 20:00-6:00"

If given, each time script starts compatision of new table it checks if current time is outside given range. That allows to add entry in crontab to start i.e. at 20:00 and the script stops its work after crossing given boundary. You can provide multiple time ranges. For example --range "Mon-Fri 21:00-6:00" --range "Sat-Sun" will allow to run the script every work day at night and whole day during a weekend.

=item -t, --test

Prints list of tables that will be compared and exits.

=item --maxload NUMBER

If defined then every minute the script will connect to every database and will check the avarage actve session count for last minute for each instance. If any instance will have higher active sessions average than given the script will terminate immediately.

=item --auxuser user[/password]

If --maxload is provided then averages are taken from gv$active_session_history. Sometimes separate user has privileges to query that table.

=item --statefile FILENAME

If provided then each time the script terminates and there are more tables to be compared the pending table list and current results are stored in given file. It will be load during next run and comparision process will be restarted.

=item --pidfile FILENAME

Stores pid of current process. If you start another script instance with the same file it will check if there is process running with stored pid and terminates if there is one.

=item -f, --logfile FILENAME

Stores all output messages to given file. The file is opened in APPEND mode.

=item -v, --verbose

Increases verbosity level. You may add multiple -v to increase the level.

=item --continueonly

When enabled, the script starts its work only if there are tables left to process in state file.

=item -h, --help

Displays this message.

=back

=head1 EXAMPLES

perl DbCompare.pl --db db1=user/passwd@host1/service --db db2=user/passwd@host2/service --schema data_owner --range "Mo-Su 1-19:50" --logfile DbCompare.log  --state statefile.dat --maxload 10 --auxuser system --pidfile dbcompare.pid --parallel 8 --exclude DATA_OWNER.NONREPLICATEDTBL --remap db2=DATA_OWNER.TAB%:DATA_USER.TAB%

perl DbCompare.pl --basedb user/passwd@host1/service --db user/passwd@host3/service2 --schema=data_%.TA% --range="Mo-Fri 3-12:50" --logfile DbCompare.log --state statefile.dat --maxload 10 --comparekey %

perl DbCompare.pl --db user/passwd@host1/service --db user/passwd@host3/service2 --schema DATA_OWNER --exclude "DATA_OWNER.%TABLES" --parallel 8

=head1 REPORTING BUGS

 Report bugs to rj.cork@gmail.com

