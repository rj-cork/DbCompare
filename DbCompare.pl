#!/usr/bin/perl -w

# DbCompare.pl - script and manual for comparing data in multiple 
#		   schemas/tables across multiple databases
# Version 1.14_2
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


require 5.8.2;

use strict;
use Cwd;
use File::Basename;
use lib (dirname(Cwd::abs_path($0)));
use DataCompare;
use Storable;
use Data::Dumper;
use Getopt::Long;
use Pod::Usage;
use Pod::Text;
use POSIX qw(strftime :sys_wait_h :signal_h);
use IO::Select;
use IO::Handle;
use IO::Pipe;

####################################################################################
#
# 1. GetOpts
# 2. check time range
# 3. generate list of tables for given schemas, if exists state file read them from there
# 4. apply exclude 
# 5. remap
# 6. check time range
# 7. fork
# 8. setup arguments using @ARGV array
# 9. child starts comparision DataCompare::DataCompare()
# 10. wait for child/output from child
# 11. check database load
# 12. save state
# 13. go to 6
#
####################################################################################

use constant ERROR => -2;
use constant WARNING => -1;
use constant DEBUG => 1;
use constant DEBUG1 => 1;
use constant DEBUG2 => 2;
use constant DEBUG3 => 3;
use constant DEBUGNOLOG => 128;
use constant DEBUGLOGONLY => 256;

my $SQL_TIMEOUT = 20; #20 secs for connect and sql execution
my $DEBUG = 0;
my %DATABASES;
my @SCHEMAS;
my @EXCLUDES;
my $BASEDB;
my @TIME_RANGES;
my @MAPS;
my %TABLE_MAPPINGS;
my %TABLE_CMP_COLUMNS;
my %TABLE_EXCLUDE_COLUMNS;
my @CMP_COLUMNS;
my @CMP_KEY_ONLY;
my $MAX_PARALLEL = 3; #how many parallel processed objects, there can be only 1 in preparing/execute state, 
			#the rest parallel processes can be receiving data only, set to 1 to disable adaptive parallelisation
my $TEST_ONLY = 0;
my $STATE_FILE;
my $LOG_FILE :shared; #needed for lock, DataCompare.pm loads threads
my $PARALLEL = 4;
my $MAX_LOAD_PER_INST = 0;
my $CONTINUE_ONLY = 0; #if 1 do not start unless there is job left to do from previous run
#my $WORKER_PROCESS;
my $PARTITION_FILTER = ''; #works only with oracle 12 and above
my $SKIP_PARTS_ROW_LIMIT = 5*1000*1000; #switch to partitions for tables bigger than 8mln rows -> taken from stats!
my $PID_FILE;
my %CHILDREN; #hash containing all children information $child_pid => pipe, state of child (prepare|execute|receive) 
my $ROUNDS = 5; #second stage retries

sub PrintMsg {
	my $m = shift;
	my $lvl = 0;	
	my $log_only = 0;
	my $no_file = 0;

	if ($m=~ /^-?\d/) { #cast $m to number
		$lvl = int($m);
		$m = '';
	}
	
	if ($lvl > 0 && $lvl & DEBUGNOLOG) { #if set dont log it into the logfile
		$no_file = 100;
		$lvl &= ~DEBUGNOLOG;
	}

	if ($lvl > 0 && $lvl & DEBUGLOGONLY) { #if set log it into the logfile only
		$log_only = 100;
		$lvl &= ~DEBUGLOGONLY;
	}

	return if ($lvl > $DEBUG); #skip if current debug level is lower than lvl of the message

	$m = '[ERROR] ' if ($lvl == ERROR);
	$m = '[WARNING] ' if ($lvl == WARNING);

	if ($lvl < 0) {
		print STDERR $m, @_;
	} else {
		print STDOUT $m, @_ if ($log_only == 0);
	}

	if (defined($LOG_FILE) && $no_file == 0) {
		if ( ! -e $LOG_FILE || ( -f $LOG_FILE && -W $LOG_FILE ) ) {

			lock($LOG_FILE);
			my $f;

			open $f,">>$LOG_FILE";
			print $f $m, @_;
			close $f;

	        } else {
			print STDERR "[ERROR] PrintMsg(): File $LOG_FILE is not a regular or writeable file. Exiting.";
	        }
	}

}

sub GetParams {
	my @dbs;
	my $basedb;
	my $auxuser;
	my $auxpass;

	my $help = 0;
	my $i = 0;

	GetOptions ('db|d=s' => \@dbs,
	    'basedb|primary=s' => \$basedb,
	    'auxuser=s' => \$auxuser,
	    'schema|table|s=s' => \@SCHEMAS,
	    'comparecolumn|comparecol|c=s' => \@CMP_COLUMNS,
	    'comparekey=s' => \@CMP_KEY_ONLY,
	    'continueonly' => \$CONTINUE_ONLY,
            'verbose|v+' => \$DEBUG,
	    'parallel|p=i' => \$PARALLEL,
	    'maxparallel=i' => \$MAX_PARALLEL,
	    'remap|m=s' => \@MAPS,
	    'exclude|e=s' => \@EXCLUDES,
	    'range|r=s' => \@TIME_RANGES,
	    'dry|test|t' => \$TEST_ONLY,
            'rounds=i' => \$ROUNDS,
	    'statefile|state|f=s' => \$STATE_FILE,
	    'outputfile|logfile|log|o|l=s' => \$LOG_FILE,
	    'maxload=i' => \$MAX_LOAD_PER_INST,
	    'pid|pidfile=s' => \$PID_FILE,
	    'skippartsrowlimit' => \$SKIP_PARTS_ROW_LIMIT,
	    'partitionfilter=s' => \$PARTITION_FILTER,
	    'help|h' => \$help) or $help=100;

	if ($help) {
		#print STDERR ">",Cwd::abs_path($0),"|",$0,">",`pwd`,"+",((caller())[1]),"\n";
                #pod2usage(-input => 'script/DbCompare.pl' , -verbose => 2, -noperldoc => 1, -exitval => 1, -output  => \*STDOUT);
                pod2usage( -verbose => 2, -noperldoc => 1, -exitval => 1, -output  => \*STDOUT);
                #pod2usage(-input => ((caller())[1]), -verbose => 2, -noperldoc => 1, -exitval => 1, -output  => \*STDOUT);
        }

	if ((defined($basedb) && scalar(@dbs) == 0) || (!defined($basedb) && scalar(@dbs) < 2)) {
		PrintMsg ERROR, "GetParams(): At least 2 database connections are needed.\n";
		exit 1;
	}
	
	if ($PARALLEL > 32 || $PARALLEL < 1) {
		PrintMsg ERROR, "GetParams(): --parallel outside valid range 1 to 32.\n";
		exit 1;
	}


	if ($auxuser) {
		if ($auxuser =~ /([\w\d]+)(?:\/([\w\d]+))?/) {
			($auxuser,$auxpass) = ($1,$2);
			$auxpass = DataCompare::GetPass("Password for $auxuser:") if (!$auxpass);
		} else {
			PrintMsg ERROR, "GetParams(): invalid --auxuser. Should be --auxuser=someuser/somepassword\n";
			exit 1;
		}
	}		

	foreach my $d ($basedb, @dbs) {
		my $dbname;
		my %db;
		
		next if (not defined $d);

		if ($d =~ /(?:([\w\d]+)=)?([\w\d]+)(?:\/(\S+))?\@([\w\d\.\-]+)(?::(\d+))?\/([\.\-\w\d]+)/) {
			   #alias, user, pass, host, port, service
			($db{'ALIAS'}, $db{'USER'}, $db{'PASS'}, $db{'HOST'}, $db{'PORT'}, $db{'SERVICE'}) = ($1, $2, $3, $4, $5, $6);
		} else {
			PrintMsg ERROR, "GetParams(): Connect string $d doesn't match the right format: [alias=]user[/pass]\@host[:port]/service\n";
			exit 1;
		}

		if ($auxuser) {
			$db{'AUXUSER'} = $auxuser;
			$db{'AUXPASS'} = $auxpass;
		}		

		if (not defined($db{'PORT'})) {
	                $db{'PORT'} = 1521;
	        }

		if (not defined($db{'PASS'})) {
			$db{'PASS'} = DataCompare::GetPass("Password for $d:");
		}

		if (not defined($db{'ALIAS'})) { #is there name-alias given for this connection string?
			$db{'ALIAS'} = "DB$i";
		} else {
			$db{'ALIAS'} = uc($db{'ALIAS'});
		}
		$dbname = $db{'ALIAS'};
		$BASEDB = $dbname if (not defined($BASEDB)); #primary

		#if (defined($PROGRESS{$dbname})) { # check if name $db{'ALIAS'} is already taken
		#	print "ERROR: alias $db{'ALIAS'} for $d is already taken. Please choose another.\n";
		#	exit 1;
		#}


		$DATABASES{$dbname} = \%db;
		$DATABASES{$dbname}->{'NAME'} = $dbname;

		$i++;
	}

	if (scalar(@SCHEMAS) == 0) {
		PrintMsg ERROR, "GetParams(): Schema name is needed. --schema=schema1\n";
		exit 1;
	} 
	
	#if ($PARTITION_FILTER) {
	#	PrintMsg "GetParams() ddd: ($PARTITION_FILTER)\n";
	#}
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
}

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

sub VerifyTime { # returns 1 if current time matches any given time range
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
					PrintMsg DEBUG2, "VerifyTime(): $i\n";
					if ($i =~ /(\w+)\-(\w+)/) {
						$r1 = ResolveDay($1);
						$r2 = ResolveDay($2);
						PrintMsg DEBUG2, "VerifyTime(): Day: $r1 - $r2\n";
					} elsif ($i =~ /(\w+)/) {
						$r1 = $r2 = ResolveDay($1);
						PrintMsg DEBUG2, "VerifyTime(): Day: $r1\n";
					} else {
						PrintMsg ERROR, "VerifyTime(): Error parsing days\n";
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
                                        PrintMsg DEBUG2, "VerifyTime(): $j\n";

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
						PrintMsg ERROR,  "VerifyTime(): Error parsing time: $j\n";
					}

					my $t = $lt[2]*60+$lt[1];
					PrintMsg DEBUG3,  "VerifyTime(): time ",int($t/60),":",$t%60,", check in <",int($t1/60),":",$t1%60,",",int($t2/60),":",$t2%60,">\n";

					if ($t1 > $t2) { # 22-5
					      if ( ($t1 <= $t) || ($t2 >= $t) ) {
						 PrintMsg DEBUG3,  "VerifyTime(): hour match\n";
						 $hour_match = 10;
						 last;
					      }
					} else { # 9-12 or 9-9
					      if ( ($t1 <= $t) && ($t <= $t2) ) {
						 PrintMsg DEBUG3,  "VerifyTime(): hour match\n";
						 $hour_match = 10;
						 last;
					      }
					}
				}
			} else {
				$hour_match = -10; #hour is not given - implicytly assume that hour matched
			}

			if ($hour_match == -10 && $day_match == -10) {
				PrintMsg ERROR,  "VerifyTime(): Invalid time range: $r\n";
			} elsif ($hour_match == 0 || $day_match == 0) {
				PrintMsg DEBUG1,  "VerifyTime(): didn't match $r\n";
			} else {
				PrintMsg DEBUG2, 'VerifyTime(): Current ',strftime("%A %H:%M",@lt)," matched range: $r\n";
				return 1;
			}
		} else {
			PrintMsg ERROR,  "VerifyTime(): --range wrong format\n";
			exit 1;
		}
	}
	return 0;
}

#output is an array of 
sub GetTableList {
	my $dbh = shift;
	my $schema = shift;
	my $table = shift;
	my %list;
	my $sql = "select t.owner,t.table_name,p.partition_name,p.high_value,p.partition_position,t.num_rows from all_tables t left join all_tab_partitions p on t.owner=p.table_owner and t.table_name=p.table_name where ";

	if ($PARTITION_FILTER) {
		$sql = q{
WITH
  FUNCTION get_high_value_as_tm(p_owner in varchar2, p_table_name in varchar2, p_partition_name in varchar2) RETURN date IS
        v_high_value varchar2(1024);
        i number;
        v_time timestamp;
  BEGIN
    select high_value into v_high_value from all_tab_partitions                
        where  table_owner=upper(p_owner) and table_name = upper(p_table_name) and partition_name = upper(p_partition_name);
    execute immediate 'select ' || v_high_value || ' from dual' into v_time;
    RETURN v_time;

    EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN others THEN
	RETURN NULL;
  END;
select * from (
select t.owner,t.table_name,p.partition_name,p.partition_position,t.num_rows,high_value,get_high_value_as_tm(t.owner,t.table_name,p.partition_name) high_value_date from all_tables t left join all_tab_partitions p on t.owner=p.table_owner and t.table_name=p.table_name
) t where };
        #raise_application_error(-20001, 'Cannot get last date for: '||p_table_name||'.'||p_partition_name||': '||sqlerrm);
		$sql .= $PARTITION_FILTER.' and ';
	}

	$dbh->{LongReadLen} = 1000; # high_value is long
					#when choosing partition using high value substract 1 second or less - 1/86400
		#select count(*) from DATA_OWNER.TAB partition for (TO_DATE(' 2016-11-17 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')-1/86400);
		#select count(*) from DATA_OWNER.TAB partition (SYS_P29878);

	if (!defined($schema)) {
		PrintMsg ERROR, "GetTableList(): schema not defined!\n";
		exit 1;
	}

	$schema = uc($schema);
	PrintMsg DEBUG3, "GetTableList(): $schema\n";

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
	PrintMsg DEBUG3, "GetTableList(): $sql\n";

	my %slice;
	my $r = $dbh->selectall_arrayref($sql, { Slice => \%slice } );
	foreach my $row (@{$r}) {
		my $p = $row->{OWNER}.'.'.$row->{TABLE_NAME};

		#if (defined ($row->{PARTITION_NAME})) {
		#switch to partitions only where $row->{NUM_ROWS}>$SKIP_PARTS_ROW_LIMIT 
		if (defined ($row->{PARTITION_NAME}) && $row->{NUM_ROWS} > $SKIP_PARTS_ROW_LIMIT) {
			if (defined($row->{HIGH_VALUE}) && ($row->{HIGH_VALUE} =~ /TIMESTAMP|TO_DATE/ || defined($row->{HIGH_VALUE_DATE}))) { #is it interval/range?
			#if (defined($row->{HIGH_VALUE}) && $row->{HIGH_VALUE} =~ /TIMESTAMP/ ) { #is it interval/range?
				$p .= '.'.$row->{PARTITION_POSITION};
				$list{$p} = $row->{HIGH_VALUE}." - INTERVAL '1' SECOND";
			} else {
				$p .= '.'.$row->{PARTITION_NAME};
				$list{$p} = '';
			}
		} else {
			$list{$p} = '';
		}

		PrintMsg DEBUG2, "GetTableList(): table: $p ",($list{$p})?",part high val: $list{$p} \n":"\n";
	}
	return \%list;
}

sub ConnectToDatabases { #for what? TODO
	my $dbs = shift;
	
	foreach my $db (keys %{$dbs}) {
		my $dbh = DataCompare::ConnectToDatabase($dbs->{$db});
		if (not defined($dbh)) {
			PrintMsg ERROR, "ConnectToDatabases(): Connection failed to $db->{NAME}\n";
			exit 1;
		}
		$dbs->{$db}->{_DBH_} = $dbh;
	}
}

sub GetAllTables {
	my $dbs = shift;
	my $schemas = shift;
	my %table_list;

	foreach my $db (keys %{$dbs}) {
		my $dbh = DataCompare::ConnectToDatabase($dbs->{$db});
		if (not defined($dbh)) {
			PrintMsg ERROR, "GetAllTables(): Connection failed to $db->{NAME}\n";
			exit 1;
		}
		foreach my $s (@{$schemas}) {
			my $h_ref;
				# $1 (obligatory) - schema, $2 (optional) - table 
			if ($s =~ /^([\w\d\?\*%]+)(?:\.([\w\d\$\?\*%]+))?$/) { 
				$h_ref = GetTableList($dbh, $1, $2);
			} else {
				$h_ref = GetTableList($dbh, $s);
			}
			map {$table_list{$db}->{$_} = $h_ref->{$_}} keys %{$h_ref};
		}
	}

	return \%table_list;
}

sub MatchRegexp { #match case insensitive
	my $m = shift;
	my $reg = shift; #if undef then return as matched

	return 2 if (!defined($reg) || $reg eq '');
	
	return 0 if (!defined($m)); #didn't match if data to match doesn't exist but pattern (exclude) exists
	
        #change %,*,? into perl regexp characters
        $reg =~ s/\*/\.\*/g;
        $reg =~ s/\?/\./g;
	$reg =~ s/%/\.\*/g;
	$reg =~ s/\$/\\\$/g;
        return 1 if ($m =~ /^$reg$/i);

	return 0; #didnt match
}

#v1.02 changed partition to column
sub RemoveExcludedTables {
	my $tables = shift;
	my $e_schema = shift; #optional/may be undef
	my $e_table = shift;
	my $e_col = shift; #optional/may be undef
	my $db = shift; #for printing messages only
	my $exclusions = 0;

	foreach my $t (sort keys %{$tables}) {
		if ($t =~ /^(?:([\w\d]+)\.)([\w\d\\\$]+)(?:\.([\w\d]+))?$/) {
			my ($sch,$ta,$pa) = ($1,$2,$3);
			#print STDERR "$t => $sch|$ta|$pa\n";
			if ( MatchRegexp($sch, $e_schema) && MatchRegexp($ta, $e_table) ) { #&& MatchRegexp($pa, $e_part) ) {
				#table matched

				#do we want exclude column only?
				if (defined($e_col)) {
					my $c = '';
					$c = ',' if ($TABLE_EXCLUDE_COLUMNS{$t});
					$TABLE_EXCLUDE_COLUMNS{$t} = $c.$e_col;
				} else { #no column for exclusion -> lets exclude table
					delete $tables->{$t};
					PrintMsg DEBUG2, "RemoveExcludedTables(): Table $t excluded for $db\n";
				}
				$exclusions++;
			}
		} else {
			PrintMsg ERROR, "RemoveExcludedTables(): Error parsing table $t\n";
		} 
	}
	return $exclusions;
}

sub RemoveExcludesFromList {
	my $table_list = shift;
	my $excludes = shift;
	my $exclusions = 0;

# --exclude [db2=][schema.]tablename[.partition]  
	foreach my $e (@{$excludes}) {
		PrintMsg DEBUG2, "RemoveExcludesFromList(): Processing exclude $e\n";
		$exclusions = 0;
		$e = uc($e);
		#TODO: you never know if it is schema.table or table.partition, may be it should be :partition not .partition
		#   for now we will leave schema as obligatory so something1.something2 is always schema.table
		# 	$1(optional)-dbalias, $2-schema,    $3(opt)-table,     $4(opt)-partition 
		#   since v1.02 $4 will not be partition, it will be a column, RemoveExcludedTables is updated as well
		#	$1(optional)-dbalias, $2-schema,    $3(opt)-table,     $4(opt)-column - without wildcards
		if ($e =~ /^(?:([\w\d]+)=)?([\w\d\?\*%]+)(?:\.([\w\d\\\$\?\*%]+))?(?:\.([\w\d]+))?$/) {
			my ($dba, $schema, $table, $column) = ($1, $2, $3, $4);
			#empty/undef value means all 
			#print "$dba, $schema, $table, $column\n";

			if (defined($column) && defined($dba)) {
				PrintMsg ERROR, "RemoveExcludesFromList(): You cannot exclude column for only one database. Remove dbalias in $e\n";
				exit 1;
			}

			foreach my $db (sort keys %{$table_list}) {
				if (defined($dba) && not defined($table_list->{$dba})) { #is there table list for this dbalias?
					PrintMsg ERROR, "RemoveExcludesFromList(): Invalid dbalias $dba given in $e\n";
					exit 1;
				}

				next if (defined($dba) and $db ne $dba); #skip other databases if dbalias is given for the exclude

				if (not defined($dba)) { #dbalias is not defined, process every db
					$exclusions += RemoveExcludedTables($table_list->{$db}, $schema, $table, $column, $db);
				} else { #dbalias is defined for this exclude
					$exclusions += RemoveExcludedTables($table_list->{$dba}, $schema, $table, $column, $dba);
				}
			}
		} else {
			PrintMsg ERROR, "RemoveExcludesFromList(): Invalid exclude $e \n";
			exit 1;
		}

		if ($exclusions == 0) {
			PrintMsg WARNING, "RemoveExcludesFromList(): $e didn't match any schema/table/column.\n";
		}	
	}
}

sub SubstRegexp { #subst case insensitive
	my $f = shift; #PATTERN
	my $t = shift;  #REPLACEMENT
	my $m_ref = shift; #reference to input string, stores modified string

	if ((defined($f) && !defined($t)) || (!defined($f) && defined($t))) {
		PrintMsg WARNING, "SubstRegexp(): wrong matching clause, either PATTERN or REPLACEMENT is missing\n";
		return 0;
	}

	#remap schema1.tab1:schema2.tab2
	return 2 if (!defined($f) && !defined($t)); #no remapping clause/instrucitons -> return as matched
	
	return 0 if (!defined(${$m_ref})); #didn't match if data to match doesn't exist but pattern (exclude) exists

	return 3 if ($f eq $t and $t eq ${$m_ref}); # from and to remap clauses are the same, as well as checked string

	if ($f !~ /[%\*]/ && $t =~ /[%\*]/) {
		PrintMsg WARNING, "SubstRegexp(): wrong remap clause ($f:$t), wildcards in replacement are not allowed if not found in pattern \n";
		return 0;
	}

	PrintMsg DEBUG3, "SubstRegexp(): pattern: ",Def($f,' replacement: ',$t,' input string: ',${$m_ref}),"\n";
	# %a?a -> (.*)a(.)a
	$f =~ s/\?/\(\.\)/g; 
	$f =~ s/[%\*]/\(\.\*\)/g;	
	PrintMsg DEBUG3, "SubstRegexp(): pattern changed to perl reg: $f\n";
	my $c=0;
	my @o = ${$m_ref} =~ /^$f$/i; #match pattern against input string
	PrintMsg DEBUG3, "SubstRegexp(): the pattern matched against ${$m_ref} gave [",join(',',@o),"] \n";

	if (scalar(@o) == 0) { # $$m_ref didn't match $f
		PrintMsg DEBUG3, "SubstRegexp(): no match, returning\n";
		return 0;
	}


	$t =~ s/[%\?\*]/{$o[$c++]}/ge; #substitute % ? * with matches from previous match
	#implement SOME%TABLE_0? -> \1SOMETABLE_NO_\2 or $1SOMETABLE_NO_$2
	$t =~ s/[\$\\](\d+)/{$o[$1-1]}/ge;

	PrintMsg DEBUG3, "SubstRegexp(): replacement is: $t\n";

	${$m_ref} = $t;
	return 1;
	#$t =~ s/\\/\$/g; #\1b\2b -> $1b$2b
	## %b?b -> "$1b$2b"
	#$t=~s/[%\*\?]/{$c++;"\$$c"}/eg;
	#$t = '"'.$t.'"';
        #return 1 if (${$m_ref} =~ s/^$f$/$t/iee);
	#return 0; #didnt match
}

#returns all non undef array elements
sub Def {
	grep { defined } @_;	
}

sub RemapTables {
	my ($tables,$f_s,$f_t,$f_p,$t_s,$t_t,$t_p,$db) = @_;
	#$f_s, $f_p, $t_s, $t_p #optional/may be undef
	my $mappings = 0;

	foreach my $t (sort keys %{$tables}) {
		if ($t =~ /^(?:([\w\d]+)\.)([\w\d\$]+)(?:\.([\w\d]+))?$/) {
			my ($sch,$ta,$pa) = ($1,$2,$3);
			PrintMsg DEBUG2, "RemapTables(): remap for $t with pattern: ",Def($f_s,',',$f_t,',',$f_p),
			     '; replacement: ',Def($t_s,',',$t_t,',',$t_p);
	
			#was there any mapping applied before for this table?
			my $r = $TABLE_MAPPINGS{$db}->{$t};

			if ( defined ($r) ) { #yes - we will use output of previous mapping as imput for this one
									# so --remap clauses will stack
				$sch = $r->{'SCHEMA'} if ($r->{'SCHEMA'});
				$ta = $r->{'TABLE'} if ($r->{'TABLE'});
				$pa = $r->{'PARTITION'} if ($r->{'PARTITION'});

				PrintMsg DEBUG2, '; matching against already remapped: ',Def($sch,',',$ta,',',$pa),"\n";
			} else {
				PrintMsg DEBUG2, '; matching against: ',Def($sch,',',$ta,',',$pa),"\n";
			}

			if ( SubstRegexp($f_s, $t_s, \$sch) && SubstRegexp($f_t, $t_t, \$ta) && SubstRegexp($f_p, $t_p, \$pa) ) {
				#TODO: should we remap partition which is auto-generated? it is skipped in PrepareArgs()
				#table matched, lets add new name to TABLE_MAPPINGS
				$TABLE_MAPPINGS{$db}->{$t} = {'SCHEMA'=>$sch, 'TABLE'=>$ta, 'PARTITION'=>$pa};
				$mappings++;

				PrintMsg DEBUG2, "RemapTables(): Table $t will be mapped to ", $sch, '.',$ta,
					(defined ($pa))?'.'.$pa:'',
					"\n";
			}
		} else {
			PrintMsg ERROR, "RemapTables(): Error parsing table $t\n";
			exit 1;
		}
	}
	return $mappings;
}

sub RemapAllTables {
	my $table_list = shift;
	my $maps = shift;
# --remap [db2=]schema1[.tablename1][.partition0]:schema2[.tablename2][.partition1]
# todo?: --remap [db2=]schema1[.tablename1][.partition0]:schema2[.tablename2][.partition1],partition2

	foreach my $m (@{$maps}) {
		my $mappings = 0;
		$m = uc($m);
		#$1(optional)-dbalias, $2 orig, $3 remap to
		if ($m =~ /^(?:([\w\d]+)=)?(.+):(.+)$/) {
			my ($dbalias,$from,$to) = ($1,$2,$3);
			my ($from_sch,$from_tab,$from_par,$to_sch,$to_tab,$to_par);

			$dbalias = uc($dbalias) if (defined($dbalias));
			if (defined($dbalias) && not defined($DATABASES{$dbalias})) {
			#if (defined($dbalias) && not defined($table_list->{$dbalias})) {
				PrintMsg ERROR, "RemapAllTables(): Invalid dbalias $dbalias in $m remap caluse\n";
				exit 1;
			}
				#$1-schema, $2(opt)-table, $3(opt)-partition
			if ($from =~ /^([\w\d\?\*%]+)(?:\.([\w\d\$\?\*%]+))?(?:\.([\w\d\?\*%]+))?$/) {
				($from_sch,$from_tab,$from_par) = ($1,$2,$3);
			} else {
				PrintMsg ERROR, "RemapAllTables(): Invalid from specification in $m remap caluse\n";
				exit 1;
			}

				#$1-schema, $2(opt)-table, $3(opt)-partition
			if ($to =~ /^([\w\d\?\*%]+)(?:\.([\w\d\$\?\*%]+))?(?:\.([\w\d\?\*%]+))?$/) {
				($to_sch,$to_tab,$to_par) = ($1,$2,$3);
			} else {
				PrintMsg ERROR, "RemapAllTables(): Invalid to specification in $m remap caluse\n";
				exit 1;
			}

			#empty/undef value means all 
			#print "$dba, $schema, $table, $partition\n";
			foreach my $db (sort keys %DATABASES) {
				next if (defined($dbalias) and $db ne $dbalias); #skip other databases if dbalias is given for the remap 

				if (!defined($dbalias) && $db ne $BASEDB) { #dbalias is not defined, process every db except PRIMARY/BASEDB
					$mappings += RemapTables($table_list->{$BASEDB}, $from_sch,$from_tab,$from_par, $to_sch,$to_tab,$to_par, $db);
					#$mappings += RemapTables($table_list->{$db}, $from_sch,$from_tab,$from_par, $to_sch,$to_tab,$to_par, $db);
				} elsif (defined($dbalias)) { #dbalias is defined for this exclude
					$mappings += RemapTables($table_list->{$BASEDB}, $from_sch,$from_tab,$from_par, $to_sch,$to_tab,$to_par, $dbalias);
					#$mappings += RemapTables($table_list->{$dbalias}, $from_sch,$from_tab,$from_par, $to_sch,$to_tab,$to_par, $dbalias);
				}
			}
		} else {
			PrintMsg ERROR, "RemapAllTables(): Invalid remap clause $m\n";
			exit 1;
		}

		if ($mappings == 0) {
			PrintMsg WARNING, "RemapAllTables(): $m didn't match any schema/table/partition.\n";
		}	
	}
}

# --comparecol [schema1.][tablename1.]column_name --comparecol [schema1.][t?ble%1.]column_name -> relates to BASEDB/PRIMARY
sub MarkCmpColumns {
	my $table_list = shift;
	my $cmp_list = shift;

	foreach my $c (@{$cmp_list}) {
		my $c_count = 0;
		$c = uc($c);
		#$1(optional) - schema, $2(opt) - table, $3 column for comparision
		if ($c =~ /^(?:([\w\d\?\*%]+)\.)?(?:([\w\d\$\?\*%]+)\.)?([\w\d]+)$/) {
			my ($c_sch,$c_tab,$c_col) = ($1,$2,$3);

			foreach my $t (sort keys %{$table_list->{$BASEDB}}) {
				if ($t =~ /^(?:([\w\d]+)\.)([\w\d\$]+)(?:\.([\w\d]+))?$/) {
					my ($sch,$ta,$pa) = ($1,$2,$3);

					if ( MatchRegexp($sch, $c_sch) && MatchRegexp($ta, $c_tab) ) {
						#table matched, store CMP column
						$TABLE_CMP_COLUMNS{$t} = $c_col;
						PrintMsg DEBUG2, "MarkCmpColumns(): Column for $t table comparision is $c_col\n";
						$c_count++;
					}
				} else { #shouldn't happen, list of tables is generated from database
					PrintMsg ERROR, "MarkCmpColumns(): Error parsing table $t\n";
					exit 1; 
				}
			}
		} else {
			PrintMsg ERROR, "MarkCmpColumns(): Invalid compare clause $c\n";
			exit 1;
		}

		if ($c_count == 0) {
			PrintMsg WARNING, "MarkCmpColumns(): $c didn't match any schema/table\n";
		}	
	}
}

# --comparekey [schema1.]t?ble%1 -> relates to BASEDB/PRIMARY
sub MarkCmpKeyOnly {
	my $table_list = shift;
	my $cmp_list = shift;

	foreach my $c (@{$cmp_list}) {
		my $c_count = 0;
		$c = uc($c);
		#$1(optional) - schema, $2 - table
		if ($c =~ /^(?:([\w\d\?\*%]+)\.)?([\w\d\$\?\*%]+)$/) {
			my ($c_sch,$c_tab) = ($1,$2);

			foreach my $t (sort keys %{$table_list->{$BASEDB}}) {
				if ($t =~ /^(?:([\w\d]+)\.)([\w\d\$]+)(?:\.([\w\d]+))?$/) {
					my ($sch,$ta,$pa) = ($1,$2,$3);


					if ( MatchRegexp($sch, $c_sch) && MatchRegexp($ta, $c_tab) ) {
						#if column choosed for comparison do not overwrite it
						if (!defined($TABLE_CMP_COLUMNS{$t})) {
							$TABLE_CMP_COLUMNS{$t} = ' ';#space means use pk/uk only not a column
						}
						PrintMsg DEBUG2, "MarkCmpKeyOnly(): $t table marked for pk/uk comparision only.\n";
						$c_count++;
					}
				} else { #shouldn't happen, list of tables is generated from database
					PrintMsg ERROR, "MarkCmpKeyOnly(): Error parsing table $t\n";
					exit 1; 
				}
			}
		} else {
			PrintMsg ERROR, "MarkCmpKeyOnly(): Error parsing $c\n";
			exit 1;
		}

		if ($c_count == 0) {
			PrintMsg WARNING, "MarkCmpKeyOnly(): $c didn't match any schema/table\n";
		}	
	}
}

sub LoadStateFile {
        my $f = shift;
	my $dbs = shift;
	my %state_file;

        return if(!defined $f);

        if ( -f $f && -R $f ) { 
                %state_file = %{retrieve($f)} or do { PrintMsg ERROR, "LoadStateFile(): Error reading state file $f: $!"; exit 1; }
        } else {
		if (-e $f) { # and the file exists
	                PrintMsg ERROR, "LoadStateFile(): File $f is not a regular or readable file.";
			exit 1;
		} 
		return (undef, undef); #new file needs to be created
        }

	if (!defined($state_file{TABLES})) {
		PrintMsg ERROR, "LoadStateFile(): invalid state file.\n";
		exit 1;
	}

	foreach my $db (keys %{$dbs}) {
		if (!defined($state_file{TABLES}->{$db})) {
			PrintMsg ERROR, "LoadStateFile(): Invalid state file. Missing tables for $db.\n";
			exit 1;
		}
	}
	
	return ($state_file{TABLES}, $state_file{REPORT}, $state_file{LOGFILE});
}

sub CreatePidfile {
	my $PF;

        return if(!defined $PID_FILE);
        
        if ( -e $PID_FILE ) { #exists?
		if ( -f $PID_FILE && -W $PID_FILE ) { #plain file, writable ?
			open PF, "$PID_FILE" or do { PrintMsg ERROR, "CreatePidfile(): Cant open $PID_FILE for reading\n"; exit 1; };       
                        my $pid = <PF>;
			close PF;

			if ( ! -e "/proc/cpuinfo" ) {
				PrintMsg ERROR, "CreatePidfile(): /proc filesystem not mounted.\n"; 
                                exit 1;
			}
			if ( -e "/proc/$pid") {
				PrintMsg ERROR, "CreatePidfile(): $pid is still running. Exiting.\n"; 
				exit 1;
			}
		} else {
			PrintMsg ERROR, "CreatePidfile(): $PID_FILE is not writable or is not a plain file. Exiting.\n"; 
                        exit 1;
		}
        } 

	#there is no $PID_FILE, we can (re)create it
	open PF, ">$PID_FILE" or do { PrintMsg ERROR, "CreatePidfile(): Cant open $PID_FILE for writing\n"; exit 1; };
	print PF $$;
	close PF;
}

sub SaveStateFile {
	my %sav;

        my $f = shift;
	$sav{'TABLES'} = shift;
	$sav{'REPORT'} = shift;
	$sav{'LOGFILE'} = shift;

        return if(!defined $f);

	PrintMsg DEBUG, "SaveStateFile(): Saving state in $f\n";
        
        if ( ! -e $f || ( -f $f && -W $f ) ) { 
		if (scalar(keys(%{$sav{'TABLES'}->{$BASEDB}})) > 0) { #do we have any tables left to process?
	                store \%sav, $f or do { PrintMsg ERROR,  "SaveStateFile(): Error writing current state to file $f: $!"; exit 1; }
		} else { #nothing to do, it was the last one
			unlink $f or PrintMsg ERROR,  "SaveStateFile(): Cannot remove state file $f: $!";
		}
        } else {
                PrintMsg ERROR, "SaveStateFile(): File $f is not a regular or writeable file.";
		exit 1;
        }
}

sub CheckLoadLimits { #check database load / child mem usage(TODO?)
	my $dbs = shift;
	#active session count per instance - avg from last minute
	my $sql = "select round(avg(c),1) ACT_SESS_AVG,inst_id from ( ";
	$sql .= "select count(*) c,SAMPLE_ID,inst_id from gv\$ACTIVE_SESSION_HISTORY where sample_time>sysdate - INTERVAL '1' MINUTE group by sample_id, inst_id";
	$sql .= ") group by inst_id";

	return 0 if ($MAX_LOAD_PER_INST == 0); #do not check anything if it is turned off

        PrintMsg DEBUG1, "CheckLoadLimits(): limit is $MAX_LOAD_PER_INST per instance.\n";

	foreach my $db (keys %{$dbs}) {

		my ($dbh, $r, %slice);

		my $mask = POSIX::SigSet->new( SIGALRM ); # signals to mask in the handler
		my $action = POSIX::SigAction->new(
		       sub { die "connect timeout\n" },        # the handler code ref
		       $mask,
		       # not using (perl 5.8.2 and later) 'safe' switch or sa_flags
		);
		my $oldaction = POSIX::SigAction->new();
		sigaction( SIGALRM, $action, $oldaction );
		eval {
			eval {
				alarm($SQL_TIMEOUT); # seconds before time out
				my %d = %{$dbs->{$db}};
				if ($d{'AUXUSER'}) {
					$d{'USER'} = $d{'AUXUSER'};
					$d{'PASS'} = $d{'AUXPASS'};
				}
				$dbh = DataCompare::ConnectToDatabase(\%d);
				$dbh->{RaiseError} = 1;

				if (not defined($dbh)) {
					return -1;	
				}

				$r = $dbh->selectall_arrayref($sql, { Slice => \%slice } );
			};
			alarm(0); # cancel alarm (if connect worked fast)
			die "$@\n" if $@; # connect died
		};
		sigaction( SIGALRM, $oldaction );  # restore original signal handler
		if ( $@ ) {
			if ($@ eq "connect timeout\n") {
				PrintMsg ERROR, "CheckLoadLimits(): Connection and sql execution timeout for",
						" $dbs->{$db}->{NAME}. Exiting at", POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),"\n";
			} else { #connection died
				PrintMsg ERROR, "CheckLoadLimits(): Connection or sql execution failed for ",
						"$dbs->{$db}->{NAME}. Exiting at ", POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),"\n";
			}
			return -1;		
		}
	
		if (!defined($r)) {
			PrintMsg ERROR, "CheckLoadLimits(): Cannot execute sql query. Exiting at ", POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),"\n";
			return -1;		
		}

		foreach my $row (@{$r}) {
			#next if (!defined($row) || !defined($row->{ACT_SESS_AVG}));
			if ($row->{ACT_SESS_AVG} > $MAX_LOAD_PER_INST) {
				PrintMsg ERROR, "CheckLoadLimits(): 1 minute avg for instance $row->{INST_ID} @ $db is bigger than limit $MAX_LOAD_PER_INST. Current value is: $row->{ACT_SESS_AVG}. Time: ", POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),"\n";
				return -2;		
			} else {
				PrintMsg DEBUGNOLOG, "CheckLoadLimits(): 1 minute avg for instance $row->{INST_ID} @ $db is $row->{ACT_SESS_AVG}. Time: ", POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),"\n";
			}
		}
	}
	return 0;
}

sub SigTerm {
	PrintMsg ERROR,"[$$] SigTerm(): $? ",POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),"\n";
#	$SIG{CHLD} = \&SigTerm;
#	$SIG{ALRM} = \&SigTerm;
	Terminate();
}

sub Terminate {
	PrintMsg ERROR,"[$$] Terminate() - ",POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),"\n";

	if (defined($CHILDREN{'child'})) { #is it child?
		PrintMsg "Child process $$ - exiting.\n";
	} else { #or parent?
		ShutdownWorkers(keys %CHILDREN);
		PrintMsg "Parent process $$ - exiting.\n";
        	if ( $PID_FILE && -e $PID_FILE &&  -f $PID_FILE && -W $PID_FILE ) { 
			PrintMsg "Removing $PID_FILE.\n";
			unlink $PID_FILE;
		}
	}
	exit 1;
}

sub ShutdownWorkers {
	my @pids = @_;

	@pids = keys %CHILDREN if (scalar(@pids) == 0);

	PrintMsg "ShutdownWorkers(): ", join(', ',@pids), "\n";

	if (not defined($CHILDREN{'child'})) { #do we have a child or are we the child

		foreach my $p (@pids) {
			PrintMsg WARNING, "Killing child $p by SIGTERM.\n";
			kill 15, $p;
			sleep 3;
		        my $child = waitpid($p, WNOHANG);
			if ($child ==  $p) {
				PrintMsg WARNING, "Killed\n";
			} else {
				kill 9, $p;
				sleep 2;
		        	waitpid($p, 0); #zombie cleanup
				PrintMsg WARNING, "Killed with 9\n";
			}
		}

	} else { #shouldn't happen
		PrintMsg WARNING, "ShutdownWorkers(): called from a child. Shouldn't happen. \n";
	}

}

sub ReportResults {
	my $tables = shift;
	my $report = shift;
	my $marker_tm = shift;
	my $final = 0;
	my $sum_outof = 0;

	if ($TEST_ONLY) {
		print "This was dry run only\n" ;
		return 0;
	}

	map {my $s=$report->{$_}->{summary}->{sum_outof}; $sum_outof += $s if($s)} (keys (%{$report}));

	my $left = scalar keys (%{$tables});
	if ($left == 0) {
		PrintMsg "\n===== Final results =====\n";
		PrintMsg "\nTotal out of sync records: $sum_outof\n\n";
		$final = 10;
	} else {
		PrintMsg "\n===== Partial results =====\n";
	}

	foreach my $k (sort keys (%{$report})) {
		my $t = $report->{$k}->{summary};
		my $p = $report->{$k}->{parts};
		my $parts_count = 0;

		next if ($left && $t->{timestamp} ne $marker_tm); #skip if not final results or result not from this run
		$parts_count = scalar(keys %{$p}) if (defined($p));

		if ($p && $final == 0) {#for final results print summary for tables only - no partitions
			foreach my $k_p (sort keys %{$p}) {
				#check if there is full information captured for this partition
				next if (not defined($p->{$k_p}->{exectime})); 
				#skip if not final results or result not from this run
				next if ($left && $p->{$k_p}->{timestamp} ne $marker_tm); 

				my $high_val = $p->{$k_p}->{highval};
				if (defined($high_val))	{
					$high_val =~ s/^TO_DATE\(\' (\S+) (\S+)\'.+/$1 $2/; 
					PrintMsg DEBUGLOGONLY, "$k partition: $k_p, high value: $high_val\n";
				} else {
					PrintMsg DEBUGLOGONLY, "$k partition: $k_p\n";
				}
				
				PrintMsg DEBUGLOGONLY, "\tRows: ",$p->{$k_p}->{rows},
			#		" 2nd stage synced: ",$p->{$k_p}->{nd_synced},
			#		" 2nd stage deleted: ",$p->{$k_p}->{nd_deleted},
			#		" 2nd stage out of sync: ",$p->{$k_p}->{nd_outof},
					"\t 2nd stage loops: ",$p->{$k_p}->{nd_execs},
					"\t Out of sync: ",$p->{$k_p}->{sum_outof},
					"\t Invalid checks: ",$p->{$k_p}->{sum_bad},
					"\t Time: ",$p->{$k_p}->{exectime},"sec",
					"\n";
			}
		}

		#check if there is full information captured for this table
                next if (not defined($t->{exectime}));

		PrintMsg "$k summary: \n";
		PrintMsg "",
			"\tOut of sync: ",$t->{sum_outof},
			"\tRows: ",$t->{rows},
		#		" 2nd stage synced: ",$t->{nd_synced},
		#		" 2nd stage deleted: ",$t->{nd_deleted},
		#		" 2nd stage out of sync: ",$t->{nd_outof},
			#	"\t 2nd stage loops: ",$t->{nd_execs},
			#	"\t Invalid checks: ",$t->{sum_bad},
			#	"\t Number of processed partitions: ",$parts_count,
			#	"\t Time: ",$t->{exectime},"sec",
			"\n\n";
	}
	PrintMsg "===== $left tables/partitions left to check. =====\n" if ($left);
}

#- table name 
#  - partitions
#   - part high value|part name
#    - row count (from BASEDB)
#    - 2nd synced
#    - 2nd deleted
#    - 2nd out of sync
#    - 2nd no of passes
#    - sum out of sync
#    - sum bad
#  - summary
#   - row count (from BASEDB)
#   - 2nd synced
#   - 2nd deleted
#   - 2nd out of sync
#   - 2nd no of passes
#   - sum out of sync
#   - sum bad
   
sub PopulateReport {
	my $report = shift;
	my $in = shift;
	my $table_name = shift;
	my $table_list = shift;
	my $marker_tm = shift; 

	if ($in =~ /\sERROR:/ || $in =~ /\sWARNING:/) {
		PrintMsg "$in";
	} else {
		PrintMsg DEBUG1|DEBUGNOLOG, "$in";
	}
	
	if ($table_name !~ /^(?:([\w\d\$]+)\.)([\w\d\$]+)(?:\.([\w\d]+))?$/) {
		PrintMsg ERROR, "PopulateReport(): cannot parse table name $table_name\n";
	}
	my $tab = "$1.$2";
	my $part = $3; #may be undef

	if ($part) {
		$report->{$tab}->{parts}->{$part}->{timestamp} = $marker_tm;
		$report->{$tab}->{parts}->{$part}->{highval} = $table_list->{$BASEDB}->{$table_name};
	}
	$report->{$tab}->{summary}->{timestamp} = $marker_tm;

	if ($in =~ /FirstStageWorker\[$BASEDB\] finished, total rows checked: (\d+)/) {
		if ($part) {
			$report->{$tab}->{parts}->{$part}->{rows} = $1;
		}
		$report->{$tab}->{summary}->{rows} += $1;
	} elsif ($in =~ /SecondStageLookup: (\d+) synced, (\d+) deleted, (\d+) out of sync/) {
		if ($part) {
			$report->{$tab}->{parts}->{$part}->{nd_synced} += $1;
			$report->{$tab}->{parts}->{$part}->{nd_deleted} += $2;
			$report->{$tab}->{parts}->{$part}->{nd_outof} += $3;
			$report->{$tab}->{parts}->{$part}->{nd_execs}++;
		}
		$report->{$tab}->{summary}->{nd_synced} += $1;
		$report->{$tab}->{summary}->{nd_deleted} += $2;
		$report->{$tab}->{summary}->{nd_outof} += $3;
		$report->{$tab}->{summary}->{nd_execs}++;
	} elsif ($in =~ /FinalResults:  out of sync: (\d+), bad: (\d+)/) {
		if ($part) {
			$report->{$tab}->{parts}->{$part}->{sum_bad} += $2;
			$report->{$tab}->{parts}->{$part}->{sum_outof} += $1;
		}
		$report->{$tab}->{summary}->{sum_bad} += $2;
		$report->{$tab}->{summary}->{sum_outof} += $1;
	} elsif ($in =~ /FINISH .+ \((\d+):(\d+):(\d+)\)/) {
		my $t = $1*3600+$2*60+$3;
		if ($part) {
			$report->{$tab}->{parts}->{$part}->{exectime} += $t;
		}
		$report->{$tab}->{summary}->{exectime} += $t;
	}
	return $report;
	print Dumper($report);
}

sub PrepareArgs {
	my @args;
	my $dbs = shift;
	my $table_list = shift;
	my $table_name = shift;
	my $info = "$BASEDB=";
	my ($sch,$tab,$par);
	
	if ($table_name =~ /^([\w\d\?\*%]+)\.([\w\d\$\?\*%]+)(?:\.([\w\d\?\*%]+))?$/) {
		($sch,$tab,$par) = ($1,$2,$3);

		push @args, '--table', "$sch.$tab";
		$info .= "$sch.$tab";

		if ($par) {
			if ($table_list->{$BASEDB}->{$table_name}) {
				$info .= " PARTITION: ".$table_list->{$BASEDB}->{$table_name};
				push @args, '--partitionfor', $table_list->{$BASEDB}->{$table_name};
			} else {
				$info .= " PARTITION: $par";
				push @args, '--partition', $par;
			}
		}
	} else {
		PrintMsg ERROR, "Error parsing table $table_name"; #shouldn't happen, $table_name is taken from database
	}

	foreach my $d (keys %{$dbs}) {
		my $r = $dbs->{$d};

		push @args, '--db';
		push @args, $r->{'ALIAS'}.'='.$r->{'USER'}.'/'.$r->{'PASS'}.'@'.$r->{'HOST'}.':'.$r->{'PORT'}.'/'.$r->{'SERVICE'};

		#--map db2=data_owner.table
		$r = $TABLE_MAPPINGS{$d}->{$table_name};
		if (defined($r)) {
			push @args, '--map', "$d=".$r->{SCHEMA}.'.'.$r->{TABLE};
			$info .= "\n$d=".$r->{SCHEMA}.'.'.$r->{TABLE};
			if (defined($r->{PARTITION})) {
				if (not defined($table_list->{$BASEDB}->{$table_name})) { #no mapping if partition for is used to determine partition
					#--mappartition db2=P_0001    - this is new name for partiion given in --partition PART_0001
					push @args, '--mappartition', "$d=".$r->{PARTITION};
					$info .= " PARTITION: ".$r->{PARTITION};
				} else { #no mapping if partition for is used to determine partition
					$info .= " PARTITION: ".$table_list->{$BASEDB}->{$table_name};
				}
			}
		} else {
			$info .= "\n$d=$sch.$tab" if ($d ne $BASEDB);
		}
	}

	if ($TABLE_EXCLUDE_COLUMNS{$table_name}) {
		push @args, '--excludecol', $TABLE_EXCLUDE_COLUMNS{$table_name};
		$info .= "\nExcluding column ".$TABLE_EXCLUDE_COLUMNS{$table_name}."if sha1 used";
	}

	if (defined($TABLE_CMP_COLUMNS{$table_name})) {
		if ($TABLE_CMP_COLUMNS{$table_name} eq ' ') { 
			push @args, '--keyonly';
			$info .= "\nUsing pk/uk";
		} else {
			push @args, '--comparecol', $TABLE_CMP_COLUMNS{$table_name};
			$info .= "\nUsing column ".$TABLE_CMP_COLUMNS{$table_name};
		}
	} else {
		$info .= "\nUsing sha1";
	}

	if ($LOG_FILE) {
		push @args, '--logfile', $LOG_FILE;
	}	

	push @args, '--parallel', $PARALLEL;

	for (my $i=0; $i<$DEBUG && $i<10; $i++) {
		push @args, '--verbose'; #it stacks
	}	

	if (defined $ROUNDS) {
		push @args, '--rounds', $ROUNDS;
	}	


	PrintMsg $info, "\n" if ($TEST_ONLY);

	return @args;
}

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

sub SpawnNewProcess {
	my ($args_ref, $table_name, $processed_tables_ref, $selector) = @_;

	PrintMsg DEBUG1, "Starting DataCompare() with args: \n\t", join("\n\t--",split(' --', join(' ', @{$args_ref}))), "\n";

	die "too many children" if (scalar(keys(%CHILDREN)) > 10); #hardcoded safety limit

	my $pipe = new IO::Pipe or die "$!";
	my $pid = fork();
	$processed_tables_ref->{$table_name} = $pid; #needs to be removed when successfully processed

	#### this is child
	if ($pid == 0) { #we are child
		%CHILDREN = ('child' => 1); #clear CHILDREN hash, we are child
		SetProcessName(@{$args_ref}); #change process name, it will contain info on processing state

		$SIG{INT} = \&SigTerm;
		$SIG{TERM} = \&SigTerm;
		$SIG{CHLD} = \&SigTerm; #= "IGNORE"; ?? if ignored: (TODO)
		$SIG{ALRM} = \&SigTerm;

		$pipe->writer(); #close $pipe_r;
		$pipe->autoflush(1);
		open STDOUT, ">&".fileno($pipe) or die "$!";
		open STDERR, ">&".fileno($pipe) or die "$!";
		#perl 5.10 has GetOptions with args array as parameter but we want to be compatible with 5.8 
		#so here is dirty hack
		@ARGV = @{$args_ref};  

		DataCompare::DataCompare(1); #proceed with table comparision
		exit 0;
	} 
	#### end of child

	#### parent 

	# setup new child 
	$pipe->reader(); #close($pipe_w); #the child has this end of pipe
	#$pipe->blocking(0);

	$CHILDREN{$pid} = { 'state' => 'prep', #prep|exec|recv
				'pipe' => $pipe, 
				'fileno' => fileno($pipe), 
				'tablename'=> $table_name,
			 }; 

        return $pipe;
}

sub ProcessAllTables {
	my ($table_list, $report, $dbs) = @_;

	my $table_name;
	my $marker_tm = time; #we will recognize which results are from actual run and which are loaded from state file
	my $t = time;
	my $t_par = time;
	my $selector = IO::Select->new();
	my %processed_tables; #hash with key = table being processed, value = pid
	my $terminate = 0;

	while(1) { 

		#always get first element/table from the array except tables marked in %processed_tables as being processed
		$table_name = (sort grep {!defined($processed_tables{$_})} keys %{$table_list->{$BASEDB}})[0];

		#break the loop if there are no more tables left and nothing is processed -> we're done
		if (!defined($table_name) && scalar(keys(%processed_tables)) == 0) {
			print STDERR "no more to process\n";
			last;
		}

		if (VerifyTime(@TIME_RANGES) == 0) {
			PrintMsg ERROR, "Stopping, I'm outside available time slots. (",POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),")\n";
			last;
		}

		if (defined($table_name)) { #do we have next table to process?

			PrintMsg "\nProcessing $table_name ",POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),"\n";
		
			if ($TEST_ONLY) {
				delete $table_list->{$BASEDB}->{$table_name};
				next;
			}

			my @args = PrepareArgs($dbs, $table_list, $table_name);

			$selector->add(SpawnNewProcess(\@args, $table_name, \%processed_tables));
		
		}

		my $was_eof = 0; #if read gets eof it means child died and needs to be scrubbed
		#wait in this loop until it can proceed with new table/partition
		while (1) {
			my $kid = 0;

			#scrub all terminated children
			while ( ($kid = waitpid(-1, WNOHANG)) > 0 ) {

				PrintMsg DEBUG1, "ProcessAllTables(): scrubing $kid, exit code: $? \n";

				#fix race condition between removing finished child and select that returns some leftovers
				my $pi = $CHILDREN{$kid}->{'pipe'};
				if (!eof($pi)) { 
					my $l;
					while($l = <$pi>) {
						PrintMsg WARNING, "ProcessAllTables(): [$kid] purging pipe: $l\n";
						$report = PopulateReport ($report, $l, $table_name, $table_list, $marker_tm);
					}
					$selector->remove($pi) or die "IO::Select->Remove: $!";
					close($pi);
				}

				$table_name = undef;
				delete $CHILDREN{$kid};
				foreach my $t (keys %processed_tables) { #find which table $kid was processing
					if ($processed_tables{$t} == $kid) {
						delete $processed_tables{$t}; #clear as being processed
						$table_name = $t;# what is the tablename of finished process
						PrintMsg DEBUG1, "ProcessAllTables(): $t is not processed any more.\n";
						last;
					}
				}


				if (not defined($table_name)) {
					PrintMsg ERROR, "ProcessAllTables(): Cannot find what was the last processed table. Exiting. \n";
					exit 1;
				}			

				if ($? != 0) {
				#error situation
					if ($? & 127) {
						PrintMsg ERROR, 'Child died with signal ('.($? >> 8)."/$?). It will be restarted.\n";
						#Restart the child and continue?
					} else {
						PrintMsg ERROR, 'Error from child '.($? >> 8)." ($?)\n";
						exit 1;
					} 
				} else { #no error so this one is completed
					PrintMsg DEBUG, "ProcessAllTables(): $table_name successfully processed.\n";
					delete $table_list->{$BASEDB}->{$table_name};
					SaveStateFile($STATE_FILE, $table_list, $report, $LOG_FILE);
				}

			}

			#check if we need to stay here or we can start new child 
			last if (scalar(keys(%CHILDREN)) == 0); #start new process imediatelly

			#calculate every 2 sec whether enable processing in parallel
			if (time - $t_par >=3 && scalar(keys(%CHILDREN)) < $MAX_PARALLEL) { # parallel process
				$t_par = time;
				my %states = ( 'prep'=>0, 'exec'=>0, 'recv'=>0 );
				foreach my $c (keys %CHILDREN) { #calculate how many children is in each state
					$states{$CHILDREN{$c}->{'state'}}++;
				}
				PrintMsg DEBUG2, "ProcessAllTables(): children states: ", join(",",%states), "\n";
				if ($states{'prep'} + $states{'exec'} == 0) {
					PrintMsg DEBUG, "ProcessAllTables(): No process in prep/exec state out of ",scalar(keys(%CHILDREN)),".\n";
					last; # proceed with new child
				}
			}
	
			$was_eof = 0;

			#read from children's STDOUT/ERR
			while (my @pipes = $selector->can_read(.2)) { #with sleep 1/5sec
				foreach my $p (@pipes) { 

					my $c = undef;
					foreach my $z (keys %CHILDREN) {
						if ($CHILDREN{$z}->{'pipe'} == $p && $CHILDREN{$z}->{'fileno'} == fileno($p)) {
							$table_name = $CHILDREN{$z}->{'tablename'};
							$c = $z;
						}
					}

					if (not defined($c)) {
						PrintMsg ERROR, "ProcessAllTables(): Cannot find what process sent last line. Exiting. \n";
						PrintMsg ERROR, "ProcessAllTables(): [", $p->getline(), "].\n";
						PrintMsg DEBUG1, "fno: ",fileno($p)," ",Dumper(\%CHILDREN),"\n";
						exit 1;
					}			
#TODO: if to while?
					if (my $in = $p->getline()) {
						if ($in =~ /FirstStageWorker: sql execution finished across all workers/) {
							$CHILDREN{$c}->{'state'} = 'recv';
						}
						$report = PopulateReport ($report, $in, $table_name, $table_list, $marker_tm);
					} elsif (eof($p)) {
						$was_eof = 1;
						$selector->remove($p) or die "IO::Select->Remove: $!";
						close($p);
					}
				}
			}

			next if ($was_eof); #check if any child died immediately

			#check load/active sessions every 1 minute 
			if ((time - $t) >= 60) {
				if (CheckLoadLimits($dbs) < 0) {
					ShutdownWorkers(keys %CHILDREN); 
					$terminate = 100;
					last;
				}
				$t = time;
			}

                }

		last if ($terminate); #worker killed, lets break main loop

	}

	ReportResults($table_list->{$BASEDB}, $report, $marker_tm);

	if ($STATE_FILE && -e $STATE_FILE && keys(%{$table_list->{$BASEDB}}) == 0) { #if no more tables to do, SaveStateFile will remove state file
		#its in case there is state file without tables to do 
		SaveStateFile($STATE_FILE, $table_list, $report, $LOG_FILE);
	}
}


###########################################################################################

$| = 1; #no buffering on stdout

$SIG{INT} = \&Terminate;
$SIG{TERM} = \&Terminate;

GetParams();
SetProcessName(@ARGV);

if (VerifyTime(@TIME_RANGES) == 0) {
	PrintMsg ERROR, "Stopping, I'm outside available time slots. (",POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),")\n";
	exit 0;
}

my $list;
my $report = {};
my $logfile;

CreatePidfile();

#TODO: do we load from file everything or starting list of table only which is re-processed?
if ($STATE_FILE) {  #for now we are reprocessing old list
	($list, $report, $logfile) = LoadStateFile($STATE_FILE, \%DATABASES);
}

if (!defined($LOG_FILE) && $logfile) { #if logfile not given use stored filename in state file
	$LOG_FILE = $logfile;
}

if (not defined($list)) { #this is fresh start, no tables in statefile or no statefile
	if ($CONTINUE_ONLY) {
		PrintMsg ERROR, "Stopping, This is fresh start and --continueonly flag enabled. (",POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),")\n";
		exit 0;
	}
	$list = GetAllTables(\%DATABASES, \@SCHEMAS);
}

RemoveExcludesFromList($list, \@EXCLUDES); #$list is reference
MarkCmpKeyOnly($list, \@CMP_KEY_ONLY);
MarkCmpColumns($list, \@CMP_COLUMNS);
RemapAllTables($list, \@MAPS); #$list is reference
ProcessAllTables($list, $report, \%DATABASES);


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

=item --maxparallel NUMBER

How many parallel processed objects. There can be only 1 in preparing/execute state, the rest parallel processes can be receiving data only, set to 1 to disable adaptive parallelisation
Defaults to 3, one in execution and two in data receiving state.

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

=item --skippartsrowlimit

Privide min number of rows for table which will have partitions checked separately.

=item -h, --help

Displays this message.

=back

=head1 EXAMPLES

perl DbCompare.pl --db db1=user/passwd@host1/service --db db2=user/passwd@host2/service --schema data_owner --range "Mo-Su 1-19:50" --logfile DbCompare.log  --state statefile.dat --maxload 10 --auxuser system --pidfile dbcompare.pid --parallel 8 --exclude DATA_OWNER.NONREPLICATEDTBL --remap db2=DATA_OWNER.TAB%:DATA_USER.TAB%

perl DbCompare.pl --basedb user/passwd@host1/service --db user/passwd@host3/service2 --schema=data_%.TA% --range="Mo-Fri 3-12:50" --logfile DbCompare.log --state statefile.dat --maxload 10 --comparekey %

perl DbCompare.pl --db user/passwd@host1/service --db user/passwd@host3/service2 --schema DATA_OWNER --exclude "DATA_OWNER.%TABLES" --parallel 8

=head1 REPORTING BUGS

 Report bugs to rj.cork@gmail.com

