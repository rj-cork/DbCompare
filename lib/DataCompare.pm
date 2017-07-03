package DataCompare;

# DataCompare package - functions for comparing data in multiple tables/table partitions
# Version 1.21
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
use warnings;
use threads;
use threads::shared;
use Thread::Semaphore;
use Thread::Queue;
use POSIX;
use DBI;
use DBD::Oracle qw(:ora_types);
use Time::HiRes;
use Getopt::Long;
use Data::Dumper;
use Storable qw(freeze thaw dclone);
use Pod::Usage;
use FileHandle;


use Logger;
use Database;

#
#  1) Read parameters GetParams()
#
#  2) For each defined database connection startup worker thread FirstStageWorker()
#        2a) Change table name if mapping is defined
#        2b) Connect to database defined in connection string for given worker
#        2c) Setup session, set RowCacheSize to  $MAX_ROWS, set NLS date/timestamp formats
#        2d) Get columns for the table. If some other worker already did it, then compare if column names, data types and nullable flag is the same
#        2e) Find what columns are in PK for the table, if there is no PK, use Unique constraint.
#            If some other worker already did it, then compare if constraint name, constraint type and column positions are the same
#                (TODO: possible bug -> constraint name shouldn't be checked in case table name mapping)
#        2f) Apply partition mapping to sql text if defined (for multiple partition use UNION ALL)
#        2g) Execute prepared sql (select on pk columns and compare_column_tm for all records that are older than 10 minutes or select on pk and
#            SHA1 creted on all non pk columns)
#        2h) For each batch of $MAX_ROWS rows
#                For each row in batch
#                        Check if SHA1 or compare_column_tm for given PK is the same in other hashes populated by other workers
#                        If they are the same, remove from all hashes (record in sync), if they are different or missing leave
#                        in DIFF hash for later checking
#
#  3) After all FirstStageWorkers are done check DIFF hashes with records saved by each worker. (FirstStageFinalCheck)
#        For each worker
#                For each row stored in DIFF for given worker
#                        Check if SHA1 or compare_column for given PK (hash key) is the same in other hashes filled by other workers
#                        If they are the same, remove from all DIFF hashes. (record is in sync)
#
#  4) Second stage comparision (SecondStageLookup)
#        Like in 6) create list of PK from all workers (DIFF hashes)
#        For each PK key
#                Go to each defined database and get current compare_column_tm or SHA1 from it. Put it into DIFF hash for given database/worker.
#                        Check if it is the same in all databases. If they are the same, remove from all hashes (record in sync).
#
#  5) Repeat step 4) $ROUNDS times with $SECONDSTAGESLEEP seconds of sleep between them. 
#     If there are no records that are frequently updated all the time and no PK columns are updated then 
#     after this step only out of sync records should stay in DIFF hashes
#
#  6) Go through all PKs from all DIFF hashes (for all databases) and check if values (SHA1/compare_column_tm) is the same.
#        Remove if they are the same. What is left is out of sync.
#
#############################################################################



$|=1;
my $LOGFILE :shared;
my %DATABASES :shared;
my %COLUMNS :shared;
my %DIFFS :shared;
my $COLSFILLED :shared;
my $RUNNING :shared;
my %PROGRESS :shared; #controls how many select result batches workers can be off each other: 5
#there is small mem leak in standard semaphore code but only few bytes for each up/down
my %MAPPINGS :shared;
my %PARTMAPPINGS :shared;
my $OUTOFSYNCCOUNTER :shared = 0;
my $CMP_KEY_ONLY :shared = 0; #if set, do not use column for comparison nor sha1 - just check pk -> records existence 
my $LIMITER :shared; #used if CMP_COLUMN defined, TODO: will it be used at all?

my $print_exec_finished :shared = 1; #flag for printing message about finished sql executing only once in all workers

my $MAX_ROWS = 10000; #how many rows to process at once
my $MAX_DIFFS = $MAX_ROWS*120; #maximum out of sync recorded records. it is sefety limit
				#so the script will not allocate whole memory if
				#something goes wrong

my $PARALLEL = ' /*+ PARALLEL(4) */ ';
my $PARTITION = ''; #compare certain partition if given
my $TABLENAME;
my $DEBUG=0;
my $PRINT_HEADER = 0; #print [schema.table.partition] header in each debug message
my $CMP_COLUMN;
my $SHIFT=10*60; #used if CMP_COLUMN defined - how much seconds back from now we are selecting data
		# 10 minutes should be enough for data to be replicated from SRC to DST so (almost)a
		# all records with timestamp<systimestamp-$SHIFT should be in sync
my $SECONDSTAGESLEEP = 30; #wait 30 seconds between each 2nd stage lookup pass
my $SECONDSTAGETRIES = 5; #how many 2nd stage lookup passes
my %EXCLUDE_COLUMNS; #columns that should be skipped, kept in hash for easy look up. 
my $CHECK_COL_TYPE = 1;
my $CHECK_COL_NULLABLE = 1;
my $SWITCH_TO_CMP_PK_IF_NEEDED = 1; #allow to switch automatically to PK/U compare mode if all columns are 
				    #in PK/UK and there is no column to calculate SHA1 

#############################################################################






sub ConnectToDatabase {
	my $d = shift;
        my $dbh;

	if (not defined($d->{'PASS'})) {
		PrintMsg("ERROR: ",$d->{'NAME'}," no password given\n");
		return undef;
	}

	if (not defined($d->{'PORT'})) {
                $d->{'PORT'} = 1521;
        }

	PrintMsg( $d->{'NAME'},": dbi:Oracle://$d->{HOST}:$d->{PORT}/$d->{SERVICE} user: $d->{USER}\n");# if ($DEBUG>0);

        $dbh = DBI->connect('dbi:Oracle://'.$d->{'HOST'}.':'.$d->{'PORT'}.'/'.$d->{'SERVICE'},
                            $d->{'USER'},
                            $d->{'PASS'});

	if (not defined($dbh)) { 
		$RUNNING = -101;
		PrintMsg( "ERROR: $DBI::errstr making connection \n");
		return undef;
	}

        return $dbh;
}

sub SessionSetup {
	my $dbh = shift;
	my $db = shift;

	$dbh->{RaiseError} = 0; #dont die imediately
        $dbh->{RowCacheSize} = $MAX_ROWS; 

	$dbh->do("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");
	$dbh->do("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF'");
#	uncomment to force buffered reads instead direct reads
#	#$dbh->do('alter session set "_parallel_cluster_cache_policy" = CACHED');
#	#$dbh->do('alter session set PARALLEL_FORCE_LOCAL = true');
#	#$dbh->do('alter session set  "_serial_direct_read" = never');
#	#$dbh->do('alter session set "_very_large_object_threshold" = 1000000000');
	my $r = $dbh->selectrow_hashref("select SYS_CONTEXT ('USERENV','INSTANCE_NAME') INST, SYS_CONTEXT ('USERENV','DB_NAME') DB from dual");
	
	$db->{'INSTANCE_NAME'}=$r->{'INST'};
	$db->{'DB_NAME'}=$r->{'DB'};
}

sub GetPrimaryKey {
	my $dbh = shift;
	my $tablename = shift;
	my $wname = shift;
	my $sql;

	$sql = "SELECT cols.table_name, cols.column_name, cons.status, cons.owner,cons.constraint_type ctype,cols.constraint_name,cols.position ";
	$sql .= "FROM all_constraints cons join all_cons_columns cols on (cols.constraint_name=cons.constraint_name) WHERE ";
	$sql .= "cons.constraint_type in ('P','U') AND cons.owner = cols.owner AND cons.status = 'ENABLED' ";

	if ($tablename =~ /(\w+)\.(\w+)/) {
		$sql .= "and cols.table_name = '$2' and cols.owner='$1' ";
	} else {
		$sql .= "and cols.table_name='$tablename' ";
	}
	$sql .= "order by ctype, constraint_name, position";

	my %slice;

	PrintMsg ("GetPrimaryKey($wname): $sql\n") if ($DEBUG>1);

	my $r = $dbh->selectall_arrayref($sql, { Slice => \%slice } );
	my $pk_found=0;
	my $u_found=0;
	my $cname;
	
	lock($COLSFILLED); #this is async process
	if ($COLSFILLED) { #pk/uniq was retrieved by some other worker, so we will compare 
		foreach my $row (@{$r}) {
			if ($row->{CTYPE} eq 'P') {
				$pk_found=1;
				if (($COLUMNS{$row->{COLUMN_NAME}}->{CONSTRAINT} ne 'P')
				    or ($COLUMNS{$row->{COLUMN_NAME}}->{CPOSITON} != $row->{POSITION})
				 #   or ($COLUMNS{$row->{COLUMN_NAME}}->{CONSTRAINT_NAME} ne $row->{CONSTRAINT_NAME})
				) {
					PrintMsg ("GetPrimaryKey($wname): $tablename/$row->{COLUMN_NAME} constraint type, column name or column position differs\n");
					return -1;
				}
			} elsif($pk_found==0 and $row->{CTYPE} eq 'U') { #there was no PK captured before
				if (not defined($cname) or $cname eq $row->{CONSTRAINT_NAME} ) {
					if (($COLUMNS{$row->{COLUMN_NAME}}->{CONSTRAINT} ne 'U') 
					    or ($COLUMNS{$row->{COLUMN_NAME}}->{CPOSITON} != $row->{POSITION}) 
					#    or ($COLUMNS{$row->{COLUMN_NAME}}->{CONSTRAINT_NAME} ne $row->{CONSTRAINT_NAME})
					) {
						PrintMsg ("GetPrimaryKey($wname): $tablename/$row->{COLUMN_NAME} constraint type, column name or column position differs\n");
						return -1;
					}
					$cname = $row->{CONSTRAINT_NAME};
					$u_found = 1;
				}
			}
		}
	} else { #save pk/uniq columns for comparision
		foreach my $row (@{$r}) {
			if ($row->{CTYPE} eq 'P') {
				$pk_found=1;
				$COLUMNS{$row->{COLUMN_NAME}}->{CONSTRAINT}='P';
				$COLUMNS{$row->{COLUMN_NAME}}->{CPOSITON}=$row->{POSITION};
				$COLUMNS{$row->{COLUMN_NAME}}->{CONSTRAINT_NAME}=$row->{CONSTRAINT_NAME};
			} elsif($pk_found==0 and $row->{CTYPE} eq 'U') { #there was no PK captured before, there is order by ctype so P will be always before U
				if (not defined($cname) or $cname eq $row->{CONSTRAINT_NAME} ) {
					$COLUMNS{$row->{COLUMN_NAME}}->{CONSTRAINT}='U';
					$COLUMNS{$row->{COLUMN_NAME}}->{CPOSITON}=$row->{POSITION};
					$COLUMNS{$row->{COLUMN_NAME}}->{CONSTRAINT_NAME}=$row->{CONSTRAINT_NAME};
					$cname = $row->{CONSTRAINT_NAME};
					$u_found = 1;
				}
			}
		}

	}

	if ($u_found+$pk_found==0) {
		PrintMsg ("GetPrimaryKey($wname): ERROR: There is neither PK nor Unique constraint enabled for table $tablename\n");
		return -1;
	}

	if ($COLSFILLED) { 
		PrintMsg ("GetPrimaryKey($wname): Constraint PK/U verified correctly\n") if ($u_found+$pk_found>0 && $DEBUG>0);
	} else { #this is first worker to retrieve PK/UK information
		if ($DEBUG>0) { 
			my $str = "GetPrimaryKey($wname): ";
			foreach my $i (sort { $COLUMNS{$a}->{CPOSITON} <=> $COLUMNS{$b}->{CPOSITON} } grep {defined $COLUMNS{$_}->{CPOSITON}} keys %COLUMNS) {
				 $cname = "(constraint name: ".$COLUMNS{$i}->{CONSTRAINT_NAME}.', type: '.$COLUMNS{$i}->{CONSTRAINT}.')';
				 $str .= "$i ";
			}
			PrintMsg ("$str $cname\n");
		}
	}

	$COLSFILLED=1;
	
	if ($CMP_KEY_ONLY == 0 && $SWITCH_TO_CMP_PK_IF_NEEDED > 0) { #switch to PK compare mode if all columns are in PK. CMP_KEY_ONLY=1
		foreach my $c (keys %COLUMNS) {
			#if the column is not to be excludes and if is not part of PK/UK then we are able to calculate sha1 if needed
			if (!defined($EXCLUDE_COLUMNS{$c}) && !defined($COLUMNS{$c}->{CONSTRAINT})) { #should be P or U if column is part of PK/U constraint 
				PrintMsg ("GetPrimaryKey($wname): Found column which is not in PK/U: $c\n") if ($DEBUG>0);
				return 0;
			}	
		}
		PrintMsg ("GetPrimaryKey($wname): All columns are included in PK/U constraint. Switching to keyonly comparison mode.\n");
		#$LIMITER = '1=1';
		$CMP_KEY_ONLY = 1;
	}	

	return 0;
}

sub GetColumns {
	my $dbh = shift;
	my $tablename = shift;
	my $wname = shift;
	my $sql;

	if ($tablename =~ /(\w+)\.(\w+)/) {
		$sql = "select OWNER,TABLE_NAME,COLUMN_NAME,DATA_TYPE,NULLABLE,COLUMN_ID from ";
		$sql .= "all_tab_columns where table_name='$2' and owner='$1'";
	} else {
		$sql = "select OWNER,TABLE_NAME,COLUMN_NAME,DATA_TYPE,NULLABLE,COLUMN_ID from ";
		$sql .= "all_tab_columns where table_name='$tablename'";
	}

	PrintMsg ("GetColumns($wname): $sql\n") if ($DEBUG>1);

	my $r = $dbh->selectall_hashref($sql, 'COLUMN_ID');
	if ( scalar(keys(%{$r})) == 0 ) {
		PrintMsg ("GetColumns($wname): ERROR: There are no columns in $tablename, is it accessible at all?\n");
		return -1;
	}

	if (scalar keys %COLUMNS) { #is it populated already? tu wiecej kolumn
		foreach my $c (keys %{$r}) { #check consistency #tu mniej - bedzie ok
			my $cn = $r->{$c}->{'COLUMN_NAME'};

			if ($CHECK_COL_TYPE && $COLUMNS{$cn}->{'DATA_TYPE'} ne $r->{$c}->{'DATA_TYPE'}) {
				PrintMsg ("GetColumns($wname): ERROR: Data types for column $cn differs\n");
				return -1;
			}
			my $n = 'N';
			$n = 'Y' if (defined $COLUMNS{$cn}->{'NULLABLE'});
			if ($CHECK_COL_NULLABLE && $r->{$c}->{'NULLABLE'} ne $n) {
				PrintMsg ("GetColumns($wname): ERROR: NULLABLE flag is inconsistnt for column $cn\n");
				return -1;
			}
		}
		if ( (scalar keys %{$r}) ne (scalar keys %COLUMNS) ) {
			PrintMsg ("GetColumns($wname): ERROR: There is different number of columns in compared tables.\n");
                        return -1;
		}
		PrintMsg ("GetColumns($wname): Columns verified correctly\n") if ($DEBUG>0);
	} else {
		foreach my $c (keys %{$r}) {
			my $cn = $r->{$c}->{'COLUMN_NAME'};

			$COLUMNS{$cn} = &share({});
			$COLUMNS{$cn}->{'COLUMN_ID'} = $c;
			$COLUMNS{$cn}->{'DATA_TYPE'} = $r->{$c}->{'DATA_TYPE'};
			$COLUMNS{$cn}->{'NULLABLE'} = 'Y' if ($r->{$c}->{'NULLABLE'} eq 'Y');
		}
		if ($DEBUG>0) {
			my $cn = "GetColumns($wname): ";
			foreach my $i (sort { $COLUMNS{$a}->{'COLUMN_ID'} <=> $COLUMNS{$b}->{'COLUMN_ID'} } keys %COLUMNS) {
				 $cn .= "$i (".$COLUMNS{$i}->{'DATA_TYPE'}.((defined $COLUMNS{$i}->{'NULLABLE'}) ? '/NULLABLE' : '').') ';
			}
			PrintMsg ("$cn\n");
		}
	}
	return 0;
}

sub SHA1Sql {
	my @pk = @_;
	my $sql = 'SELECT '.$PARALLEL.join(',',@pk).", DBMS_CRYPTO.Hash(\n";
	my $separator='';
        foreach my $i (sort { $COLUMNS{$a}->{'COLUMN_ID'} <=> $COLUMNS{$b}->{'COLUMN_ID'} } keys %COLUMNS) {

		next if (defined($COLUMNS{$i}->{CPOSITON})); #don't include this column in SHA1 if it is part of constraint PK/UK

		if (defined($EXCLUDE_COLUMNS{$i})) { #check if we want to skip this column
			PrintMsg ("SHA1Sql(): Skipping column $i\n") if ($DEBUG>1);
			next;
		}

                if ($COLUMNS{$i}->{DATA_TYPE} eq 'BLOB' || $COLUMNS{$i}->{DATA_TYPE} eq 'CLOB' ) {
			if (defined $COLUMNS{$i}->{NULLABLE}) {
	                        $sql .= $separator."DBMS_CRYPTO.Hash(NVL($i,'00'),3)\n"; #NVL(some data, some hex as alternative);
			} else {
	                        $sql .= $separator."DBMS_CRYPTO.Hash($i,3)\n";
			}
                } elsif ( $COLUMNS{$i}->{DATA_TYPE} eq 'XMLTYPE') {
			if (defined $COLUMNS{$i}->{NULLABLE}) {
				#$sql .= $separator."DBMS_CRYPTO.Hash(NVL(XMLType.getBlobVal($i,nls_charset_id('AL32UTF8')),'00'),3)\n"; 
                                # with getBlobVal parallel doesn't work, but hash is working strange way
                                $sql .= $separator."DBMS_CRYPTO.Hash(NVL2($i,XMLType.getClobVal($i),'00'),3)\n";
			} else {
	                        #$sql .= $separator."DBMS_CRYPTO.Hash(XMLType.getBlobVal($i,nls_charset_id('AL32UTF8'),3)\n";
				$sql .= $separator."DBMS_CRYPTO.Hash(XMLType.getClobVal($i),3)\n";
			}
                } else {
			if (defined $COLUMNS{$i}->{NULLABLE}) {
                        	$sql .= $separator."DBMS_CRYPTO.Hash(NVL(utl_raw.cast_to_raw($i),'00'),3)\n";
			} else {
	                        $sql .= $separator."DBMS_CRYPTO.Hash(utl_raw.cast_to_raw($i),3)\n";
			}
                }
                $separator='||';
        }       
	$sql .= ",3) CMP#VALUE FROM ";
}

sub FirstStageWorker {
	my $worker_name = shift;

	my $dbh;
	my $thisdb = $DATABASES{$worker_name};
	my $tablename = ResolveMapping($TABLENAME, $worker_name);

	my $msg = "FirstStageWorker[$worker_name] start - table: $tablename, ";
	if ($PARTMAPPINGS{$worker_name}) {
		$msg .= "partition mapped to: ".join(', ',@{$PARTMAPPINGS{$worker_name}}).", ";
	} else {
		$msg .= "partition: $PARTITION, " if ($PARTITION);
	}

	if ($CMP_COLUMN) {
		PrintMsg ("$msg compare using column $CMP_COLUMN \n");
	} elsif ($CMP_KEY_ONLY) {
		PrintMsg ("$msg compare using PK/UK columns only \n");
	} else {
		PrintMsg ("$msg compare using SHA1 on all columns\n");
	}

	{
		lock($RUNNING);
		$RUNNING++;
	}
	
	$dbh = ConnectToDatabase($thisdb);
	if (not defined($dbh)) {
		lock($RUNNING);
		$RUNNING=-102;
		return -1;
	}
	SessionSetup($dbh, $thisdb);

	{
		lock(%COLUMNS);
		if (GetColumns($dbh, $tablename, $worker_name)<0) {
			lock($RUNNING);
			$RUNNING=-103;
			return -1;
		}
		if (GetPrimaryKey($dbh, $tablename, $worker_name)<0) { #error
			lock($RUNNING);
			$RUNNING=-104;
			return -1;
		}
	}

	my @PK_COLUMNS = sort { $COLUMNS{$a}->{CPOSITON} <=> $COLUMNS{$b}->{CPOSITON} } grep {defined $COLUMNS{$_}->{CPOSITON}} keys %COLUMNS;

	PrintMsg( "FirstStageWorker[$worker_name] PrimaryKey/Unique: ", join(',',@PK_COLUMNS),"\n");

	my $sql;
	my $orderby = ' ORDER BY '.join(',',@PK_COLUMNS);

	if (defined($PARTMAPPINGS{$worker_name}) && scalar(@{$PARTMAPPINGS{$worker_name}}) > 0 && $PARTITION) {
		my $hook = '';
		foreach my $partmap (@{$PARTMAPPINGS{$worker_name}}) {
			$partmap = " PARTITION ($partmap) ";
			if (defined($CMP_COLUMN)) {
				$sql .= $hook.'SELECT '.join(',',@PK_COLUMNS).','.$CMP_COLUMN.' CMP#VALUE FROM '.$tablename.$partmap.' WHERE '.$LIMITER;
			} elsif ($CMP_KEY_ONLY) {
				$sql .= $hook.'SELECT '.join(',',@PK_COLUMNS).", 'exists' CMP#VALUE FROM ".$tablename.$PARTITION.' WHERE '.$LIMITER;
			} else {
				$sql .= $hook.SHA1Sql(@PK_COLUMNS).$tablename.$partmap;
			}
			$hook = "\n UNION ALL \n";
		}
		$sql .= $orderby;
	} else {
		if (defined($CMP_COLUMN)) {
			$sql = 'SELECT '.join(',',@PK_COLUMNS).','.$CMP_COLUMN.' CMP#VALUE FROM '.$tablename.$PARTITION.' WHERE '.$LIMITER.$orderby;
		} elsif ($CMP_KEY_ONLY) {
			$sql = 'SELECT '.join(',',@PK_COLUMNS).", 'exists' CMP#VALUE FROM ".$tablename.$PARTITION.' WHERE '.$LIMITER.$orderby;
		} else {
			$sql = SHA1Sql(@PK_COLUMNS).$tablename.$PARTITION.$orderby;
		}
	}

	PrintMsg( "[$worker_name] $sql\n") if ($DEBUG>0);

	my $prep = $dbh->prepare($sql);
	if(!defined($prep) or $dbh->err) { 
		$RUNNING = -106;
		PrintMsg( "[$worker_name] ERROR: $DBI::errstr for [$sql]\n");
		return -1;
	}

	$prep->execute();
	if(!defined($prep) or $dbh->err) { 
		$RUNNING = -107;
		PrintMsg( "[$worker_name] ERROR: $DBI::errstr for [$sql]\n");
		return -1;
	}

	my $i=0;
	my ($val,$key);
	while (my $aref = $prep->fetchall_arrayref(undef, $MAX_ROWS)) {

		my $s=2;
		{
			lock(%PROGRESS);

			my $p = $PROGRESS{$worker_name};
			foreach my $k (keys %PROGRESS) { #$p is the smallest progress for all workers
				$p = $PROGRESS{$k} if ($PROGRESS{$k}<$p);
			}

			#if the smallest progress is >0 then all are downloading data now and sql execution phase has ended across all databases
			if ($p > 0) {
				PrintMsg( "FirstStageWorker: sql execution finished across all workers.\n") if ($print_exec_finished);
				$print_exec_finished = 0;
			}

			#$s is the difference between this worker and the slowest one
			$s=$PROGRESS{$worker_name}-$p;

			until($s < 20) { #the difference cannot be bigger than 112 - around 500MB RAM per database connection for MAX_ROWS=10000
				PrintMsg( "[$worker_name] batch no. $PROGRESS{$worker_name} is $s ahead others, waiting\n") if ($DEBUG>0);
				#find worker with the smallest progress different than this one
				$p = $PROGRESS{$worker_name};
				foreach my $k (keys %PROGRESS) {
					$p = $PROGRESS{$k} if ($PROGRESS{$k}<$p);
				}
				#$s is the difference between this worker and the slowest one
				$s = $PROGRESS{$worker_name}-$p;
				cond_wait(%PROGRESS); #release lock on %PROGRESS and wait until some other worker do some work
			}
		}	
		{
			lock(%PROGRESS);
			$PROGRESS{$worker_name}++;
			cond_broadcast(%PROGRESS);
		}
				

		lock(%DIFFS);

		my $in_sync_counter=0;
		my $out_of_sync_counter=0;
		my $thisdb_only_counter=0;

		my @dbs4comparison = grep {$_ ne $worker_name} keys %DIFFS; #list of databases/workers != this one
		while (my $rref = shift(@{$aref})) {


			PrintMsg( "[$worker_name] $i ",join('|',@{$rref}),"\n") if ($DEBUG>4);
			$val = pop @{$rref}; #last column in row is value
			$key = join('|',@{$rref}); #first columns (except the last one) are key

			PrintMsg( "[$worker_name] $i key: [$key] value: $val\n") if ($DEBUG>4);

			my $thesame = 1;
			my $match = 1;
			foreach my $k (@dbs4comparison) {
				if ( defined($DIFFS{$k}->{$key}) ) { #there is matching key stored in some other DB
					if ($DIFFS{$k}->{$key} ne $val) { #key exists but value differs
						$match = 0;
						$out_of_sync_counter++;
						last;
					}
				} else {
					$match = 0;
					$thisdb_only_counter++;
					last;
				}
			}
			if ($match) { #there are the same rows in other databases
				#value stored in DIFFS{other_database} is the same as in fetched row in this database/worker
				#records are the same - we can remove it from all hashes and increase the counter
				foreach my $k (@dbs4comparison) {
					delete ($DIFFS{$k}->{$key});
				}
				$in_sync_counter++;
			} else { #there are some differences for this record (missing or different value)
				#lets add it to DIFFS hash for this worker -> its final for this pass it shouldnt be changed as $key is PK
				#it may be deleted by other workers if they find that their records are the same
				$DIFFS{$worker_name}->{$key} = $val;
			}
			$i++;
			PrintMsg( "[$worker_name] $i \n") if ($i % 1000000 == 0);
		}

		#check how many out of sync records is at the moment
		my $max_oos = 0;
		foreach my $d (keys %DIFFS) {
			my $j = scalar(keys %{$DIFFS{$d}});
			$max_oos = $j if ($j > $max_oos);
		}

		$OUTOFSYNCCOUNTER = $max_oos;
		PrintMsg( "[$worker_name] rows processed: $i; in sync: $in_sync_counter, ",
			  "out of: $out_of_sync_counter, missing in other db: $thisdb_only_counter, ",
			  "summary out of sync: $OUTOFSYNCCOUNTER \n")  if ($DEBUG>1);
	}

	$prep->finish;
	$dbh->disconnect();

	#limit so the script will not whole memory if tables are totally different
	if($OUTOFSYNCCOUNTER > $MAX_DIFFS) { 
		$RUNNING = -108;
		PrintMsg( "[$worker_name] ERROR: too many out of sync records. Max limit is $MAX_DIFFS.\n");
		return -1;
	}

	PrintMsg( "FirstStageWorker[$worker_name] finished, total rows checked: $i\n");
	{
		lock($RUNNING);
		$RUNNING--;
	}
}


sub FirstStageFinalCheck {

	my $missingsomewhere=0;
	my $outofsync=0;

	lock(%DIFFS); #shouldnt be needed

	foreach my $w (sort keys(%DIFFS)) { #for each worker/database stored in %DIFFS
		
		my @dbs4comparison = grep {$_ ne $w} keys %DIFFS; #list of databases/workers != this one
		my $in_sync_counter=0; #should be 0, because they should be cleared by workers 
		my $out_of_sync_counter=0;
		my $thisdb_only_counter=0;

		foreach my $k (keys %{$DIFFS{$w}}) { #each key left in DIFFS hash for given worker

			my $match = 1;
			foreach my $odb (@dbs4comparison) { #check whats inside others workers' hashes
				
				if ( defined($DIFFS{$odb}->{$k}) ) { #there is matching key stored in some other DB
					if ($DIFFS{$odb}->{$k} ne $DIFFS{$w}->{$k} ) { #key exists but value differs
						$match = 0;
						$out_of_sync_counter++;
						last;
					}
				} else {
					$match = 0;
					$thisdb_only_counter++;
					last;
				}
			}
			$in_sync_counter++ if($match);
		}
		$outofsync += $out_of_sync_counter;
		$missingsomewhere += $thisdb_only_counter;
		# there should be no $in_sync_counter left in DIFFS hashes
		PrintMsg( "FirstStageFinalCheck[$w]  out of sync: $out_of_sync_counter, missing in other DBs: $thisdb_only_counter/db count, bad: $in_sync_counter\n");
	}

	return $outofsync+$missingsomewhere;
}

sub SecondStagePrepSql {

	my $dbh = shift;
	my $dbname = shift;
	my $tablename = ResolveMapping($TABLENAME, $dbname);

	$dbh->{RaiseError} = 1;
        $dbh->{RowCacheSize} = 1;

	my $sql;
	my @PK_COLUMNS = sort { $COLUMNS{$a}->{CPOSITON} <=> $COLUMNS{$b}->{CPOSITON} } grep {defined $COLUMNS{$_}->{CPOSITON}} keys %COLUMNS;

	if (defined($CMP_COLUMN)) { #TODO: is limiter needed? access is by pk
		$sql = 'SELECT '.$CMP_COLUMN." FROM $tablename WHERE ".join(" and ", map { "$_=?" } @PK_COLUMNS ).' AND '.$LIMITER;
	} elsif ($CMP_KEY_ONLY) {
		$sql = "SELECT 'exists' FROM $tablename WHERE ".join(" and ", map { "$_=?" } @PK_COLUMNS ).' AND '.$LIMITER;
	} else {
		$sql = SHA1Sql(@PK_COLUMNS).$tablename.' WHERE '.join(" and ", map { "$_=?" } @PK_COLUMNS );
	}

	PrintMsg( "[$dbname] $sql\n") if ($DEBUG>1);

	my $prep = $dbh->prepare($sql);
	if($dbh->err) { 
		$RUNNING = -111;
		PrintMsg( "[$dbname] ERROR: $DBI::errstr for [$sql]\n");
		die; #no threads here, we can die
	}

	return $prep;
}

sub SecondStageGetRow {

	my $prep = shift;
	my $key = shift;
	my $dbname = shift;

	my @key_data = split(/\|/,$key);
	my $msg;

	$msg = "[$dbname] PK/U: ".(join(',',@key_data)." ") if ($DEBUG>1);

	$prep->execute(@key_data) or do {
		$RUNNING = -112;
		PrintMsg( "[$dbname] ERROR: $DBI::errstr\n");
		return undef;
	};

	my $ret_val;
	my $c = 0;
	while (my @row = $prep->fetchrow()) {
		$ret_val = pop @row; 
		$c++;
	}

	if ($c > 1) {
		$RUNNING = -113;
		PrintMsg( "[$dbname] ERROR: more than 1 record returned for PK/U key.\n");
		return undef;
	}

	PrintMsg($msg, (defined $ret_val)?"$ret_val\n":"null\n") if ($DEBUG>1);


	return $ret_val;
}

sub SecondStageLookup {
	my $synced=0;
	my $outofsync=0;
	my $deleted=0;
	my ($k, $w);

	lock(%DIFFS); #shouldnt be needed

	

	my %prep_sqls;
	my %dbhs;
# 1) connect to database again and create list of unique keys/PKs stored by all workers (out of sync records)
	my %unique_keys;
	foreach $w (keys(%DIFFS)) { #for each worker/database stored in %DIFFS

		$dbhs{$w} = ConnectToDatabase($DATABASES{$w}); #make database connection
		if (not defined($dbhs{$w})) {
			lock($RUNNING);
			$RUNNING=-120;
			return -1;
		}
		SessionSetup($dbhs{$w}, $DATABASES{$w});

		foreach $k (keys %{$DIFFS{$w}}) { #each key left in DIFFS hash for given worker
			$unique_keys{$k} = 0 if (not defined($unique_keys{$k}));
			$unique_keys{$k}++;
		}
		#prepare sqls
		$prep_sqls{$w} = SecondStagePrepSql($dbhs{$w}, $w);
		return -1 if (not defined($prep_sqls{$w}) and $RUNNING<0); #error on DBI prep
	}
# 2) for each PK check current value in all databases
	foreach $k (keys(%unique_keys)) { #for each key found in any database/worker output
		my $exists = 0;

		foreach $w (keys %DIFFS) { #check all databases/workers output
			my $newval = SecondStageGetRow($prep_sqls{$w}, $k, $w);
			return -1 if (not defined($newval) and $RUNNING<0); #error on DBI prep

			if (not defined($newval)) { #row with that key is missing in that DB
				if (defined($DIFFS{$w}->{$k})) { #but it was recorded before
					delete $DIFFS{$w}->{$k}; # update new value -> delete
				}
			} else {
				$DIFFS{$w}->{$k} = $newval; #add or update
				$exists = 1;
			}
		}

# 3) if SHA1 or timestamp column is the same then remove from all DIFF hashes
		my $val;
		my $match = 1;
		if ($exists == 0) { # key $k no longer exists in any database
			$deleted++; #increment counter
		} else {
			foreach $w (keys %DIFFS) { #check all databases/workers output
				if (defined($DIFFS{$w}->{$k})) { #there is matching key 
					if (not defined($val)) {
						$val = $DIFFS{$w}->{$k};
					} 
					if ($DIFFS{$w}->{$k} ne $val) { #key exists and the value is not the same
						$match=0;
						last;
					}
				} else {
					$match=0;
					last;
				}
			}
		}

		if ($match) { #everywhere is the same
			foreach $w (sort keys %DIFFS) {
				delete $DIFFS{$w}->{$k}; # delete this key, it is in sync everywhere
			}
			$synced++;
		} else {
			$outofsync++;
		}
	}

	PrintMsg("SecondStageLookup: $synced synced, $deleted deleted, $outofsync out of sync\n");
	foreach my $p (keys %prep_sqls) {
		$prep_sqls{$p}->finish;
		$dbhs{$p}->disconnect();
	}
	return $outofsync;
}

sub FinalResults {

	my ($k, $w);

	lock(%DIFFS); #shouldnt be needed

	my %unique_keys;
	foreach $w (keys(%DIFFS)) { #for each worker/database stored in %DIFFS
		foreach $k (keys %{$DIFFS{$w}}) { #each key left in DIFFS hash for given worker
			$unique_keys{$k} = 0 if (not defined($unique_keys{$k}));
			$unique_keys{$k}++;
		}
	}

	my $in_sync_counter=0; #should be 0, because they should be cleared by workers or SecondStageLookup
	my $out_of_sync_counter=0;

	foreach $k (keys(%unique_keys)) { #for each key found in any database/worker output
		
		my $out_line="OUT OF SYNC [$k] ";
		my $val;
		my $match=1;
		foreach $w (sort keys %DIFFS) { #check all databases/workers output
			if ( defined($DIFFS{$w}->{$k}) ) { #there is matching key 
				if (not defined($val)) {
					$val = $DIFFS{$w}->{$k};
				} 
				$out_line .= " $w: ".$DIFFS{$w}->{$k};
				if ($DIFFS{$w}->{$k} ne $val) { #key exists and the value is the same
					$match=0;
				}
			} else {
				$match=0;
				$out_line .= " $w: missing ";
			}
		}

		if ($match) {
			$in_sync_counter++;
		} else {
			$out_of_sync_counter++;
		}
		PrintMsg("$out_line\n");

	}

	PrintMsg("FinalResults:  out of sync: $out_of_sync_counter, bad: $in_sync_counter\n");

}


sub DataCompare {
	my ($print_header,$opts_ref) = @_;
	my $t = time;
	my @WORKERS;


	GetParams($opts_ref);
	
	$print_header =1;
	$PRINT_HEADER = $print_header if (defined($print_header));
	$RUNNING = 0;
	$COLSFILLED = 0;

#	if (defined($CMP_COLUMN) or $CMP_KEY_ONLY) {
		#$LIMITER = ' '.$CMP_COLUMN." < TO_TIMESTAMP('";
		#$LIMITER .= POSIX::strftime('%y/%m/%d %H:%M:%S', localtime(time-$SHIFT));
		#$LIMITER .= "','yy/mm/dd hh24:mi:ss')";
	$LIMITER = '1=1' if (not defined($LIMITER)); #we dont need limiter - lets set something that is true
#	}

	PrintMsg("START ", POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),"\n");

	my $i = 0;
	foreach my $w (sort keys %DATABASES) {
		$WORKERS[$i] = threads->create(\&FirstStageWorker, $w);
		$WORKERS[$i]->detach();
		$i++;
	}
	sleep 1;
	while($RUNNING > 0) { #wait for both workers to finish 1st pass
		sleep 1;
	}

	die "Ret: $RUNNING" if ($RUNNING < 0); #RUNNING>0, workers are processing
				#RUNNING==0, workers have finished
				#RUNNING<0, error condition, exit immediately

	FirstStageFinalCheck();

	for ($i=0;$i<$ROUNDS;$i++) {
		sleep($SECONDSTAGESLEEP) if($i);
		PrintMsg( "SecondStageLookup...\n");
		last if (SecondStageLookup() == 0); #all is in sync, no need for more checking
		last if (defined($CMP_COLUMN)); #no need for multiple checking if timestamp column is used
	}

	FinalResults();	
	PrintMsg("FINISH ", POSIX::strftime('%y/%m/%d %H:%M:%S', localtime)," (",strftime("%T",gmtime(time-$t)),")\n");

	exit 0;
}
# ---------------------------------------------------------------------------------------------------------------
my $SECONDSTAGESLEEP = 30; #wait 30 seconds between each 2nd stage lookup pass
my $SECONDSTAGETRIES = 5; #how many 2nd stage lookup passes
my $FIRST_STAGE_RUNNING :shared;
my %COLUMNS :shared; #store information about table columns, shared across all workers
my $BATCH_SIZE = 10000; #how many rows to process at once

use constant PROCESS_NAME = 'DataCompare';

sub SetProcessName {
	my $args_ref = shift;

	my $primary_db_name = $args_ref->{settings}->{primary};
	my $new_name = PROCESS_NAME;

	my $ds = $args_ref->{datasources}->{$primary_db_name};
	Logger::Terminate() if (!defined($ds));

	$new_name .= " $primary_db_name="; 
	$new_name .= $ds->{connection}->{user}.'@';
	$new_name .= $ds->{connection}->{host}.'/';
	$new_name .= $ds->{connection}->{service}.':';
	$new_name .= $ds->{name};

	foreach my $dbname (sort keys %{$args_ref->{datasources}}) {
		next if ($dbname eq $primary_db_name);

		$ds = $args_ref->{datasources}->{$dbname};
		$new_name .= " $dbname=";
	        $new_name .= $ds->{connection}->{user}.'@';
	        $new_name .= $ds->{connection}->{host}.'/';
	        $new_name .= $ds->{connection}->{service}.':';
	        $new_name .= $ds->{name};
	}

	$0 = $new_name;
}

use constant COMPARE_USING_COLUMN = 2;
use constant COMPARE_USING_SHA1 = 1;
use constant COMPARE_USING_PK = 0;

sub FirstStageWorker {
	my $data_source = shift;
	my $global_settings = shift;

	my $dbh;

	my $worker_name = $data_source->{object}->{dbname};
	my $tablename = $data_source->{object}->{table};
	my $schema = $data_source->{object}->{owner};
	my $partition_for = $data_source->{object}->{partition_for} if (defined($data_source->{object}->{partition_for}));
	my $partition_name = $data_source->{object}->{partition_name} if (defined($data_source->{object}->{partition_name}));
	my $pk_range = $data_source->{object}->{pk_range} if (defined($data_source->{object}->{pk_range}));
	my $cmp_method = COMPARE_USING_PK;

	my $msg = "FirstStageWorker[$worker_name] start - table: $tablename, ";

	if (defined($global_settings->{compare_col})) {

		$cmp_method = COMPARE_USING_PK;
		Logger::PrintMsg (Logger::DEBUG2, "$msg compare using column ".$global_settings->{compare_col});

	} elsif (defined($global_settings->{compare_hash})) {

		$cmp_method = COMPARE_USING_SHA1;
		Logger::PrintMsg (Logger::DEBUG2, "$msg compare using SHA1 on all columns");

	} else {
		Logger::PrintMsg (Logger::DEBUG2, "$msg compare using PK/UK columns only");
	}


	{
		lock($FIRST_STAGE_RUNNING);
		$FIRST_STAGE_RUNNING++;
	}
	
	$dbh = Database::Connect($data_source->{connection}, $worker_name);

	if (not defined($dbh)) {
		lock($FIRST_STAGE_RUNNING);
		$FIRST_STAGE_RUNNING = -102;
		return -1;
	}


	{ #critical section for %COLUMNS hash
		lock(%COLUMNS);

		my $checks = Database::CHECK_COLUMN_TYPE || Database::CHECK_COLUMN_NULLABLE;

		if (Database::GetColumns(\%COLUMNS, $dbh, $data_source->{object}, $worker_name, $checks) < 0) {
			lock($FIRST_STAGE_RUNNING);
			$FIRST_STAGE_RUNNING=-103;
			return -1;
		}

 		if ($cmp_method != COMPARE_USING_PK) {

			#change list of excluded columns into hash
			my %excl_col = map {$_=>1} @{$global_settings->{exclude_cols}}; 

			#switch to PK compare mode if all columns are in PK.
                	foreach my $c (keys %COLUMNS) {
				next if ($excl_col{$c}); #this column is excluded
				next if ($COLUMNS{$c}->{CONSTRAINT}); #this column is in PK/UK
				
				$cmp_method = COMPARE_USING_PK;
				last;
                        }

			if ($cmp_method == COMPARE_USING_PK) { #switched to PK compare mode
				undef $global_settings->{compare_col} if (defined($global_settings->{compare_col}));
				undef $global_settings->{compare_hash} if (defined($global_settings->{compare_hash}));
                		Logger::PrintMsg(Logger::WARNING, $worker_name, "Switching to keyonly comparison mode. All columns for comparison are in PK.");
			}
                }
        }

	my @pk_cols = sort { $COLUMNS{$a}->{CPOSITON} <=> $COLUMNS{$b}->{CPOSITON} } grep {defined $COLUMNS{$_}->{CPOSITON}} keys %COLUMNS;

	# prepare select statement
	my $sql;
	my $orderby = ' ORDER BY '.join(',', @pk_cols);

	if ($cmp_method == COMPARE_USING_COLUMN) {

		$sql = 'SELECT '.join(',',@PK_COLUMNS).','.$CMP_COLUMN.' CMP#VALUE FROM '.$tablename.$PARTITION.' WHERE '.$LIMITER.$orderby;

	} elsif ($cmp_method == COMPARE_USING_SHA1) {

		$sql = Database::SHA1Sql(@PK_COLUMNS).$tablename.$PARTITION.$orderby;

	} else { #COMPARE_USING_PK

		$sql = 'SELECT '.join(',',@PK_COLUMNS).", 'exists' CMP#VALUE FROM ".$tablename.$PARTITION.' WHERE '.$LIMITER.$orderby;

	}
>
	Logger::PrintMsg( Logger::DEBUG, $worker_name, "$sql");

	my $prep = $dbh->prepare($sql);
	if(!defined($prep) or $dbh->err) { 
		$RUNNING = -106;
		PrintMsg( "[$worker_name] ERROR: $DBI::errstr for [$sql]\n");
		return -1;
	}

	$prep->execute();
	if(!defined($prep) or $dbh->err) { 
		$RUNNING = -107;
		PrintMsg( "[$worker_name] ERROR: $DBI::errstr for [$sql]\n");
		return -1;
	}

	my $i=0;
	my ($val,$key);
	while (my $aref = $prep->fetchall_arrayref(undef, $MAX_ROWS)) {

		my $s=2;
		{
			lock(%PROGRESS);

			my $p = $PROGRESS{$worker_name};
			foreach my $k (keys %PROGRESS) { #$p is the smallest progress for all workers
				$p = $PROGRESS{$k} if ($PROGRESS{$k}<$p);
			}

			#if the smallest progress is >0 then all are downloading data now and sql execution phase has ended across all databases
			if ($p > 0) {
				PrintMsg( "FirstStageWorker: sql execution finished across all workers.\n") if ($print_exec_finished);
				$print_exec_finished = 0;
			}

			#$s is the difference between this worker and the slowest one
			$s=$PROGRESS{$worker_name}-$p;

			until($s < 20) { #the difference cannot be bigger than 112 - around 500MB RAM per database connection for MAX_ROWS=10000
				PrintMsg( "[$worker_name] batch no. $PROGRESS{$worker_name} is $s ahead others, waiting\n") if ($DEBUG>0);
				#find worker with the smallest progress different than this one
				$p = $PROGRESS{$worker_name};
				foreach my $k (keys %PROGRESS) {
					$p = $PROGRESS{$k} if ($PROGRESS{$k}<$p);
				}
				#$s is the difference between this worker and the slowest one
				$s = $PROGRESS{$worker_name}-$p;
				cond_wait(%PROGRESS); #release lock on %PROGRESS and wait until some other worker do some work
			}
		}	
		{
			lock(%PROGRESS);
			$PROGRESS{$worker_name}++;
			cond_broadcast(%PROGRESS);
		}
				

		lock(%DIFFS);

		my $in_sync_counter=0;
		my $out_of_sync_counter=0;
		my $thisdb_only_counter=0;

		my @dbs4comparison = grep {$_ ne $worker_name} keys %DIFFS; #list of databases/workers != this one
		while (my $rref = shift(@{$aref})) {


			PrintMsg( "[$worker_name] $i ",join('|',@{$rref}),"\n") if ($DEBUG>4);
			$val = pop @{$rref}; #last column in row is value
			$key = join('|',@{$rref}); #first columns (except the last one) are key

			PrintMsg( "[$worker_name] $i key: [$key] value: $val\n") if ($DEBUG>4);

			my $thesame = 1;
			my $match = 1;
			foreach my $k (@dbs4comparison) {
				if ( defined($DIFFS{$k}->{$key}) ) { #there is matching key stored in some other DB
					if ($DIFFS{$k}->{$key} ne $val) { #key exists but value differs
						$match = 0;
						$out_of_sync_counter++;
						last;
					}
				} else {
					$match = 0;
					$thisdb_only_counter++;
					last;
				}
			}
			if ($match) { #there are the same rows in other databases
				#value stored in DIFFS{other_database} is the same as in fetched row in this database/worker
				#records are the same - we can remove it from all hashes and increase the counter
				foreach my $k (@dbs4comparison) {
					delete ($DIFFS{$k}->{$key});
				}
				$in_sync_counter++;
			} else { #there are some differences for this record (missing or different value)
				#lets add it to DIFFS hash for this worker -> its final for this pass it shouldnt be changed as $key is PK
				#it may be deleted by other workers if they find that their records are the same
				$DIFFS{$worker_name}->{$key} = $val;
			}
			$i++;
			PrintMsg( "[$worker_name] $i \n") if ($i % 1000000 == 0);
		}

		#check how many out of sync records is at the moment
		my $max_oos = 0;
		foreach my $d (keys %DIFFS) {
			my $j = scalar(keys %{$DIFFS{$d}});
			$max_oos = $j if ($j > $max_oos);
		}

		$OUTOFSYNCCOUNTER = $max_oos;
		PrintMsg( "[$worker_name] rows processed: $i; in sync: $in_sync_counter, ",
			  "out of: $out_of_sync_counter, missing in other db: $thisdb_only_counter, ",
			  "summary out of sync: $OUTOFSYNCCOUNTER \n")  if ($DEBUG>1);
	}

	$prep->finish;
	$dbh->disconnect();

	#limit so the script will not whole memory if tables are totally different
	if($OUTOFSYNCCOUNTER > $MAX_DIFFS) { 
		$RUNNING = -108;
		PrintMsg( "[$worker_name] ERROR: too many out of sync records. Max limit is $MAX_DIFFS.\n");
		return -1;
	}

	PrintMsg( "FirstStageWorker[$worker_name] finished, total rows checked: $i\n");
	{
		lock($RUNNING);
		$RUNNING--;
	}
}

sub RunFirstStageWorkers {
	my $args_ref = shift;
	my @WORKERS;

	my $i = 0;
	foreach my $w (sort keys %{$args_ref->{datasources}}) {
		$args_ref->{$w}->{dbname} = $w;
		$WORKERS[$i] = threads->create(\&FirstStageWorker, $args_ref->{$w}, $args_ref->{settings});
		$WORKERS[$i]->detach();
		$i++;
	}

	sleep 1;
	while($FIRST_STAGE_RUNNING > 0) { #wait for all workers to finish 1st pass
		sleep 1;
	}

	Logger::Terminate("FIRST_STAGE_RUNNING: $FIRST_STAGE_RUNNING" if ($FIRST_STAGE_RUNNING < 0);
			#RUNNING>0, workers are processing
			#RUNNING==0, workers have finished
			#RUNNING<0, error condition, exit immediately
}

sub CoordinatorProcess { #we are forked process that is supposed to compare given table or partition
	my $out_pipe = shift; #this pipe is for sending output data to main process
	my $args_ref = shift; #this is reference to hash
	# { datasources => { dbname => { connection => { user=>user1, pass=>pass1, host=>dbhost1, port=>dbport1, service=>dbservice },
         #              	   	         object => { owner => user, table => tab1, partition_name => part1, partition_for => '..', pk_range => '..'},
		#				 name => 'user.tab1.part', #skrocona wersja partition for/partition name albo/skrocona wersja pk_range - sluzy do wyswietlania
		#				},
		#			dbname2 => {
		#				}
		#		 	}
		#		}
						#subpartition_name => '....', ??? chyba bez subpartycji, pk_range to zrobi a subpartycji interwalowych nie ma wiec
                                        #                               pk_range powinien byc bezpieczny
                #	settings => {
		#			primary => ..., which database is primary
                       #                 compare_col => ....,
                        #                compare_hash => sha1
                         #               pk_transformation => 'sub { .... } returns' ??? (w DIFFS dla kazdego klucza zmodyfikowanego powinien byc zapisany w %PK_ORIGINALS oryginalne wartosci dla porownywarki w stage 2
                          #              select_concurency => 1
                           #             stage2_rounds => 5,
                           #             stage2_sleep => 30,
                           #             exclude_cols => [col2,col3],
                           #             dont_check_type => true,
                           #             dont_check_nullable => true
                        # }

	my $start_time = time;

	Logger::SetupLogger( { RESULT_FILE_FD=>$out_pipe, RESULT_SEQUENCE=>1} );

	SetProcessName($args_ref); #change process name, it will contain info on processing state

	Logger::PrintMsg(PROCESS_NAME," started at ", POSIX::strftime('%y/%m/%d %H:%M:%S', localtime));

        RunFirstStageWorkers(); #proceed with table comparision, 1 thread per database connection

	FirstStageFinalCheck();

	for ($i=0;$i<$SECONDSTAGETRIES;$i++) {
		sleep($SECONDSTAGESLEEP) if($i);
		Logger::PrintMsg("SecondStage...");
		last if (SecondStageLookup() == 0); #all is in sync, no need for more checking
	}

	FinalResults();	

	PrintMsg(PROCESS_NAME, " stopped at ", POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),
			" time taken ", strftime("%T",gmtime(time-$start_time)));

	exit 0;
} 



1;
#DataCompare();
__END__
