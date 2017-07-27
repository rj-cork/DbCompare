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
use DBI;
use DBD::Oracle qw(:ora_types);
use Data::Dumper;


use Logger;
use Database;

$|=1;

# ---------------------------------------------------------------------------------------------------------------
my $SECONDSTAGESLEEP = 30; #wait 30 seconds between each 2nd stage lookup pass
my $SECONDSTAGETRIES = 5; #how many 2nd stage lookup passes
my $FIRST_STAGE_RUNNING :shared;
my %COLUMNS :shared; #store information about table columns, shared across all workers
my %DIFFS :shared;	#key is 'pk_col1|pk_col2|pk_col3' val is compared column or hash or 1
my %DIFFS_ORIGINAL :shared; #for 2nd stage lookups we need to decompose hash key to table pk which can be problematic
			    #in case '|' contained in any pk column. Thats why we need to keep original pk column values
				#
				#$d dbname, $t column type, $v value, $c column name
			      #for pk_transformation='sprintf("%-$1s",$v) if ($d eq 'DBSAC' and $t =~ /CHAR\((\d+)\)/)'
my $BATCH_SIZE = 10000; #how many rows to process at once
my $MAX_DIFFS = $BATCH_SIZE*10; #maximum out of sync recorded records. it is sefety limit
				#so the script will not allocate whole memory if
				#something goes wrong
my %FIRST_STAGE_BATCH_PROGRESS :shared; # shared variable for workers synchronisation at the end of each batch processing
my $WORKER_THREADS_COUNT; #number of workers/datasources

use constant PROCESS_NAME = 'DataCompare';

use constant COMPARE_USING_COLUMN = 2;
use constant COMPARE_USING_SHA1 = 1;
use constant COMPARE_USING_PK = 0;

# ---------------------------------------------------------------------------------------------------------------


sub rtrim {
	my $a = shift;
	if ($a) {
		$a =~ s/\s+$//;
	}
	return $a;
}

sub ltrim {
	my $a = shift;
	if ($a) {
		$a=~s/^\s+//;
	}
	return $a;
}

sub trim {
	return ltrim(rtrim(shift));
}

sub rpad {
        my ($a,$num,$padding) = @_;

	$padding = ' ' if (not $padding);

        if ($a && $num) {
		$a = sprintf("%-${num}s", $a);
        }
        return $a;
}

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

	# This one changes VARCHAR values to CHAR values (padded with spaces)
	# $code = 'return sprintf("%-$1s",$v) if ($t =~ /VARCHAR\((\d+)\) && $d =~ /SAC/)';
sub ColumnTransformation {
	my $column_names_ref = shift;
	my $column_values_ref = shift;
	my $column_definitions_ref = shift;
	my $code = shift;
	my $worker_name = shift;
	my @ret;

	#we will predefine variables in 'global' scope like $a and $b for sort function
	my $v;  #for value
	my $c;  #column name
	my $t;  #column type
	my $d = $worker_name;  #worker name / database alias
	for (my $i=0;$i<scalar(@{$column_values_ref});$i++) {
		$c = $column_names_ref->[$i];
		$v = $column_values_ref->[$i];
		$t = $column_definitions_ref->{$c}->{DATA_TYPE};
		$_ = $t;

		my $o = eval $code;
		Logger::Terminate("Errors in '$code': $@") if ($@);

		if ($o) {
			push @ret, $o;
		} else {
			push @ret, $v;
		}
	}

}

sub GetComparisonMethod {
	my $global_settings = shift;	

	my $cmp_method = COMPARE_USING_PK;

	if (defined($global_settings->{compare_col})) {

		$cmp_method = COMPARE_USING_PK;
		Logger::PrintMsg (Logger::DEBUG2, "$msg compare using column ".$global_settings->{compare_col});

	} elsif (defined($global_settings->{compare_hash})) {

		$cmp_method = COMPARE_USING_SHA1;
		Logger::PrintMsg (Logger::DEBUG2, "$msg compare using SHA1 on all columns");

	} else {
		Logger::PrintMsg (Logger::DEBUG2, "$msg compare using PK/UK columns only");
	}

	return $cmp_method;
}

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

	my $msg = "FirstStageWorker[$worker_name] start - table: $tablename, ";


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

		if (not defined($global_settings->{cmp_method})) {
			my $cmp_method = GetComparisonMethod($global_settings);

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

        }

	# prepare select statement
	my $sql = Database::PrepareFirstStageSelect($data_source->{object}, 
							\%COLUMNS, 
							$global_settings);

	Logger::PrintMsg(Logger::DEBUG, $worker_name, "$sql");

	my $prep = $dbh->prepare($sql);
	if(!defined($prep) or $dbh->err) { 
		$RUNNING = -106;
		Logger::PrintMsg(Logger::ERROR, $worker_name, "$DBI::errstr for [$sql]");
		return -1;
	}

	$prep->execute();
	if(!defined($prep) or $dbh->err) { 
		$RUNNING = -107;
		Logger::PrintMsg(Logger::ERROR, $worker_name, "$DBI::errstr for [$sql]");
		return -1;
	}

	my @column_names = @{$prep->{NAME}}; #names for selected columns

	my ($val,$key,$record_no,$total_out_of_sync);
	while (my $aref = $prep->fetchall_arrayref(undef, $BATCH_SIZE)) {

		{ #%DIFFS lock
			lock(%DIFFS);

			my $in_sync_counter=0;
			my $out_of_sync_counter=0; #values for keys found in other databases that differs
			my $thisdb_only_counter=0; #keys not found in other databases

			my @dbs4comparison = grep {$_ ne $worker_name} keys %DIFFS; #list of databases/workers != this one
		
			#for each record stored in $rref
			while (my $rref = shift(@{$aref})) {
				my @cols :shared;

				if (defined($global_settings->{column_transormation_from})) {
					@cols = ColumnTransformation(\@column_names, #names of selected columns
									$rref, #values for selected columns
									\%COLUMNS, #column definitions for processed table
									$global_settings->{column_transormation_from}, #transformation code
									$worker_name); #
				} else {
					@cols = @{$rref};
				} 

				#value column is 'CMP#VALUE'
				#rest of columns is pk key
				$val = pop @cols; #last column in row is value
				$key = join('|',@cols); #first columns (except the last one) are key

				#@cols = @{$rref}; 
				#values to be stored as originals 
				$DIFFS_ORIGINAL{$worker_name}->{$key} = \@cols;

				Logger::PrintMsg(Logger::DEBUG2, $worker_name, " key: [$key] value: $val");
	
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
						delete ($DIFFS_ORIGINAL{$k}->{$key});
					}
					$in_sync_counter++; #we just checked record synced across all databases
				} else { #there are some differences for this record (missing or different value)
					#lets add it to DIFFS hash for this worker -> its final for this pass it shouldnt be changed as $key is PK
					#it may be deleted by other workers if they find that their records are the same
					$DIFFS{$worker_name}->{$key} = $val;
				}
				$record_no++;
				Logger::PrintMsg(Logger::DEBUG1, $worker_name, $record_no) if ($record_no % 1000000 == 0);
			}

			#check how many out of sync records is at the moment
			my $max_oos = 0;
			foreach my $d (keys %DIFFS) {
				my $j = scalar(keys %{$DIFFS{$d}});
				$max_oos = $j if ($j > $max_oos);
			}
			$total_out_of_sync = $max_oos;

			#limit so the script will not whole memory if tables are totally different
			if($OUTOFSYNCCOUNTER > $MAX_DIFFS) { 
				$FIRST_STAGE_RUNNING=-108;
				Logger::PrintMsg(Logger::ERROR, $worker_name, "Too many out of sync records: $total_out_of_sync limit is $MAX_DIFFS.");
				$prep->finish;
				$dbh->disconnect();
				return -1;
			}
		}

		Logger::PrintMsg(Logger::DEBUG1, $worker_name, "Batch summary. Rows processed: $i, in sync: $in_sync_counter, ",
			  "out of: $out_of_sync_counter, missing in other db: $thisdb_only_counter, ",
			  "total out of sync: $total_out_of_sync");
		
		#this is end of single batch processing
		#synchronize all workers here, comparing speed is as fast as the slowest worker
		{ 
			lock(%FIRST_STAGE_BATCH_PROGRESS);

			$FIRST_STAGE_BATCH_PROGRESS{$worker_name}++; #we finished our batch here, increment the counter

			my $p = $FIRST_STAGE_BATCH_PROGRESS{$worker_name};

			while(1) {
				#check what progress is for other stages
				my $in_sync = 1;
				foreach my $k (keys %FIRST_STAGE_BATCH_PROGRESS) { 
					#wait if there is any lagging worker
					$in_sync = 0 if ($FIRST_STAGE_BATCH_PROGRESS{$k} < $p); #was != $p

					Logger::PrintMsg(Logger::DEBUG2, $worker_name, "batch progres for $k is ".$FIRST_STAGE_BATCH_PROGRESS{$k});
				}

				#are we the last worker finishing this batch?
				last if ($in_sync == 1); 

				#wait for others if not
				cond_wait(%FIRST_STAGE_BATCH_PROGRESS); 
			} 

			cond_broadcast(%FIRST_STAGE_BATCH_PROGRESS);
		}


	}

	$prep->finish;
	$dbh->disconnect();


	Logger::PrintMsg(Logger::DEBUG, $worker_name, "FirstStageWorker finished: out of sync: $total_out_of_sync, total rows: $row_no.");
	{
		lock($FIRST_STAGE_RUNNING);
		$FIRST_STAGE_RUNNING--;
	}
}

#TODO: is it necessary at all?
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
		Logger::PrintMsg(Logger::DEBUG, $worker_name, "out of sync: $out_of_sync_counter, missing in other DBs: $thisdb_only_counter/db count, bad: $in_sync_counter");
	}

	Logger::PrintMsg(Logger::INFO, $worker_name, "FirstStageWorker finished: different values: $outofsync, missing somewhere: $missingsomewhere");
	return $outofsync+$missingsomewhere;
}

sub RunFirstStageWorkers {
	my $args_ref = shift;
	my @WORKERS;

	my $i = 0;
	foreach my $w (sort keys %{$args_ref->{datasources}}) {
		$args_ref->{$w}->{dbname} = $w;
		$WORKERS[$i] = threads->create(\&FirstStageWorker, $args_ref->{$w}, $args_ref->{settings});
		$WORKERS[$i]->detach();
		{
			lock(%FIRST_STAGE_BATCH_PROGRESS);
			$FIRST_STAGE_BATCH_PROGRESS{$w} = 0;
		}
		$i++;
	}

	sleep 1;
	while($FIRST_STAGE_RUNNING > 0) { #wait for all workers to finish 1st pass
		sleep 1;
	}

	Logger::Terminate("FIRST_STAGE_RUNNING: $FIRST_STAGE_RUNNING") if ($FIRST_STAGE_RUNNING < 0);
			#RUNNING>0, workers are processing
			#RUNNING==0, workers have finished
			#RUNNING<0, error condition, exit immediately
}


sub SecondStageGetRow {

	my $prep = shift;
	my $key = shift;
	my $dbname = shift;

	my $msg;

	$msg = "PK/U: ".(join(',',@{$key}))." ";
	Logger::PrintMsg(Logger::DEBUG1, $dbname, $msg);

	$prep->execute(@{$key}) or do {
		Logger::Terminate("ERROR: $DBI::errstr");
		return undef;
	};

	my $ret_val;
	my $c = 0;
	while (my @row = $prep->fetchrow()) {
		$ret_val = pop @row; 
		$c++;
	}

	if ($c > 1) {
		Logger::Terminate("ERROR: more than 1 record returned for PK/U key. Is it PK?");
		return undef;
	}

	Logger::PrintMsg(Logger::DEBUG1, $dbname, (defined $ret_val)?"$ret_val\n":"null\n");


	return $ret_val;
}

sub SecondStagePrepare {
        my $args_ref = shift;
	my %dbhs;

        my $i = 0;
        foreach my $worker_name (sort keys %{$args_ref->{datasources}}) {
		my $dbh = Database::Connect($data_source->{connection}, $worker_name);

		if (not defined($dbh)) {
			Logger::Terminate("Not connected");
			return -1;
		}
		$dbhs{$worker_name} = $dbh;
        }

	return \%dbhs;
}

sub SecondStageLookup {
	my $dbhs_ref = shift;
        my $args_ref = shift;
	my $synced=0;
	my $outofsync=0;
	my $deleted=0;
	my $key;
	my $worker_name;

	lock(%DIFFS); #shouldnt be needed - no threads here

	my %prep_sqls;
# 1) connect to database again and create list of unique keys/PKs stored by all workers (out of sync records)
	my %unique_keys;
	foreach $worker_name (keys(%DIFFS)) { #for each worker/database stored in %DIFFS

		if (not defined($dbhs->{$worker_name})) {
			Logger::Terminate("No such worker");
			return -1;
		}

		foreach $key (keys %{$DIFFS{$worker_name}}) { #each key left in DIFFS hash for given worker
			$unique_keys{$key} = $worker_name if (not defined($unique_keys{$key}));
		}

		#prepare sqls
		my $sql = PrepareSecondStageSelect($args_ref->{datasources}->{$worker_name}->{object}, 
							\%COLUMNS, 
							GetComparisonMethod($args_ref->{settings}));

		$prep_sqls{$worker_name} = $dbh->prepare($sql);

		if($prep_sqls{$worker_name}->err) { 
			Logger::Terminate("[$worker_name] ERROR: $DBI::errstr for [$sql]"); #error on DBI prep
			die; #no threads here, we can die
		}
	}

	my @pk_columns = sort { $COLUMNS{$a}->{CPOSITON} <=> $COLUMNS{$b}->{CPOSITON} } grep {defined $COLUMNS{$_}->{CPOSITON}} keys %{$COLUMNS};

# 2) for each PK check current value in all databases
	foreach $key (keys(%unique_keys)) { #for each key found in any database/worker output
		my $exists = 0;

		foreach $worker_name (keys %DIFFS) { #check all databases/workers output

			my $orig_key = $DIFFS_ORIGINAL{$worker_name}->{$key}; #get pk for key stored in DIFF for this database

			#if record was missing for this worker then there will be no orig_key
			#and we need to recreate it from key of worker that had this record and stored orig_key

			if (not $orig_key) {
				$orig_key = $DIFFS_ORIGINAL{ $unique_keys{$key} }->{$key};
				#original key of the other worker. One, that stored this key

				if (defined($global_settings->{column_transormation_to})) {
					@{$orig_key} = ColumnTransformation(\@pk_columns, #names of selected columns
									$orig_key, #values for selected columns
									\%COLUMNS, #column definitions for processed table
									$global_settings->{column_transormation_to}, #transformation code
									$worker_name); #
				} 
			}

					#fetch from database value for original pk/uk 
			my $newval = SecondStageGetRow($prep_sqls{$worker_name}, $orig_key, $worker_name);

			#return -1 if (not defined($newval) and $FIRST_STAGE_RUNNING<0); #error on DBI prep
								#TODO terminate?

			if (not defined($newval)) { #row with currently checked key is missing in processed DB ($worker_name)
				if (defined($DIFFS{$worker_name}->{$key})) { #but it was recorded before
					delete $DIFFS{$worker_name}->{$key}; #update current state and delete key
					delete $DIFFS_ORIGINAL{$worker_name}->{$key}; #delete original key values for the database/key
				}
			} else {
				#we have new value for current row for in processed DB ($worker_name)
				if (defined($global_settings->{column_transormation_from})) {
					@{$newval} = ColumnTransformation(\@pk_columns, #names of selected columns
									$newval, #values for selected columns
									\%COLUMNS, #column definitions for processed table
									$global_settings->{column_transformation_from}); #transformation code
				} 

				#value column is 'CMP#VALUE'
				#rest of columns is pk key
				my $val = pop @{$newval}; #last column in row is value
				my $key = join('|',@{$newval}); #first columns (except the last one) are key

				#store original pk values
				$DIFFS_ORIGINAL{$worker_name}->{$key} = $newval;

				$DIFFS{$worker_name}->{$key} = $val; #add or update
				Logger::PrintMsg(Logger::DEBUG2, $worker_name, " key: [$key] value: $val");

				$exists = 1;
			}
		}

# 3) if SHA1 or timestamp column is the same then remove from all DIFF hashes
		my $v;
		my $match = 1;
		if ($exists == 0) { # key $k no longer exists in any database
			$deleted++; #increment counter
		} else {
			foreach $worker_name (keys %DIFFS) { #check all databases/workers output
				if (defined($DIFFS{$worker_name}->{$key})) { #there is matching key 
					if (not defined($val)) {
						$v = $DIFFS{$worker_name}->{$key};
					} 
					if ($DIFFS{$wworker_name->{$key} ne $v) { #key exists and the value is not the same
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
			foreach $worker_name (sort keys %DIFFS) {
				delete $DIFFS{$worker_name}->{$key}; # delete this key, it is in sync everywhere
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

@->
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

# column_transformation
# can be needed when one database has different column type in PK than the other. For example DB1 has CHAR(5) and DB2 has VARCHAR(5)
# In such case 'str  ' in DB1 can be 'str' in DB2.
# to allow PK comparison you need define function that will translate pk for DB1 to pk for DB2 
# f(char) = varchar
# g(varchar) = char
# f(g(varchar)) =  varchar 
# g(f(char)) = char

# lets say f() is rtrim: 
#	#$v is char(5)
#	return rtrim($v) #now it is varchar(5)

# lets say g() is rpad: 
#	#$v is varchar(5)
#	return rpad($v,5) #now it is char(5)

# f('str  ') = 'str'
# g(f('str  ')) = 'str  '
# g('str') = 'str  '
# f(g('str')) = 'str'

# that function is needed to correlate pk on DB1 'str  ' with pk on DB2 'str'
# that can be 
#w first stage dajemy funkcje wprost i dane zrodla
#w second stage dajemy funkcje odwrotna i dane celu

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
                         #               column_transformation_from => 'rtrim($v) if (/CHAR\((\d+)\)/)'  (w DIFFS dla kazdego klucza zmodyfikowanego powinien byc zapisany w %PK_ORIGINALS oryginalne wartosci dla porownywarki w stage 2
                         #               column_transformation_to => 'rpad($v, $1) if (/CHAR\((\d+)\)/)'  (w DIFFS dla kazdego klucza zmodyfikowanego powinien byc zapisany w %PK_ORIGINALS oryginalne wartosci dla porownywarki w stage 2
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

        RunFirstStageWorkers($args_ref); #proceed with table comparision, 1 thread per database connection

	if (FirstStageFinalCheck() > 0) {

		$SECONDSTAGETRIES = $args_ref->{settings}->{stage2_rounds} if (defined($args_ref->{settings}->{stage2_rounds}));
		$SECONDSTAGESLEEP = $args_ref->{settings}->{stage2_sleep} if (defined($args_ref->{settings}->{stage2_sleep}));
		
		my $dbhs = SecondStagePrepare($args_ref);
		for ($i=0;$i<$SECONDSTAGETRIES;$i++) {
			sleep($SECONDSTAGESLEEP) if($i);
			Logger::PrintMsg("SecondStage...");
			last if (SecondStageLookup($dbhs, $args_ref) == 0); #all is in sync, no need for more checking
		}
	}

	FinalResults();	

	PrintMsg(PROCESS_NAME, " stopped at ", POSIX::strftime('%y/%m/%d %H:%M:%S', localtime),
			" time taken ", strftime("%T",gmtime(time-$start_time)));

	exit 0;
} 



1;
#DataCompare();
__END__
