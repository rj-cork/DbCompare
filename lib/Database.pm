package Database;

# Database - functions for database operations
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
#use threads;
#use threads::shared;

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

use constant CHECK_COLUMN_TYPE = 1;
use constant CHECK_COLUMN_NULLABLE = 2;

sub GetColumns {
	my $columns = shift; #reference for 'columns' hash
	my $dbh = shift; #db handler
	my $object = shift; #reference to 'object' hash
	my $tag = shift; #tag for debug messages
	my $checks = shift; #run following checks on column list

	my $owner;
	my $table;
	my $sql;

	$owner = $object->{owner} if($object->{owner});
	$table = $object->{table};
	$tag = '' if (!defined($tag));

	$sql = "select OWNER,TABLE_NAME,COLUMN_NAME,DATA_TYPE,NULLABLE,COLUMN_ID from ";
	$sql .= "all_tab_columns where table_name='$table'";
	$sql .= " and owner='$owner'" if ($owner);

	Logger::PrintMsg(Logger::DEBUG, $sql);

	my $r = $dbh->selectall_hashref($sql, 'COLUMN_ID');

	if ( scalar(keys(%{$r})) == 0 ) {
		Logger::PrintMsg(Logger::ERROR, $tag, "Cannot get columns for $table, do I have proper grants?\n");
		return undef;
	}

	if (scalar keys %{$columns}) { #is it populated already? 

		# if there are columns retrieved already then just compare if the match
		foreach my $c (keys %{$r}) { 
			my $cn = $r->{$c}->{'COLUMN_NAME'};

			if ( $checks > 0  && (not defined($columns->{$cn})) ) {
				Logger::PrintMsg(Logger::ERROR, $tag, "Different tables, unexpected $cn column");
				return -128;
			}

			if ( ($checks & CHECK_COLUMN_TYPE) &&
			     ($columns->{$cn}->{'DATA_TYPE'} ne $r->{$c}->{'DATA_TYPE'}) {

				Logger::PrintMsg(Logger::ERROR, $tag, "Data types for column $cn differs");
				return -1 * CHECK_COLUMN_TYPE;
			}

			my $n = 'N';
			$n = 'Y' if (defined $columns->{$cn}->{'NULLABLE'});
			if ( ($checks & CHECK_COLUMN_NULLABLE) && $r->{$c}->{'NULLABLE'} ne $n) {

				Logger::PrintMsg(Logger::ERROR, $tag, "NULLABLE flag is inconsistnt for column $cn");
				return -1 * CHECK_COLUMN_NULLABLE;
			}

		}

		if ( (scalar keys %{$r}) ne (scalar keys %{$columns}) ) {
			Logger::PrintMsg(Logger::ERROR, $tag, "Different tables.");
                        return -128;
		}

		Logger::PrintMsg(Logger::DEBUG, $tag, "Columns verified correctly");

	} else {
		# read column list and store to shared hash ref.
		foreach my $c (keys %{$r}) {
			my $cn = $r->{$c}->{'COLUMN_NAME'};

			if (is_shared($columns)) {
				$columns->{$cn} = &share({});
				Logger::PrintMsg(Logger::DEBUG2, $tag, "Sharing $cn hash ref.");
			}

			$columns->{$cn}->{'COLUMN_ID'} = $c;
			$columns->{$cn}->{'DATA_TYPE'} = $r->{$c}->{'DATA_TYPE'};
			$columns->{$cn}->{'NULLABLE'} = 'Y' if ($r->{$c}->{'NULLABLE'} eq 'Y');
		}

		my $s = "Column list for ";
		$s .= $owner.'.' if ($owner);
		$s .= $table.' ';

		#sort columns by id
		my @cols = sort { $columns->{$a}->{'COLUMN_ID'} <=> $columns->{$b}->{'COLUMN_ID'} } keys %{$columns};

		foreach my $i (@cols) {
			$s .= "$i (".$columns->{$i}->{'DATA_TYPE'};
			$s .= '/NOT NULL' if (not defined $columns->{$i}->{'NULLABLE'});
			$s .= ') ';
		}

		Logger::PrintMsg(Logger::DEBUG, $tag, $s);
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
use Logger;

sub SessionSetup {
	my $dbh = shift;

	$dbh->{RaiseError} = 0; #dont die imediately
        $dbh->{RowCacheSize} = $BATCH_SIZE; 

	$dbh->do("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");
	$dbh->do("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF'");
#	uncomment to force buffered reads instead direct reads
#	#$dbh->do('alter session set "_parallel_cluster_cache_policy" = CACHED');
#	#$dbh->do('alter session set PARALLEL_FORCE_LOCAL = true');
#	#$dbh->do('alter session set  "_serial_direct_read" = never');
#	#$dbh->do('alter session set "_very_large_object_threshold" = 1000000000');
#	my $r = $dbh->selectrow_hashref("select SYS_CONTEXT ('USERENV','INSTANCE_NAME') INST, SYS_CONTEXT ('USERENV','DB_NAME') DB from dual");
	
#	$db->{'INSTANCE_NAME'}=$r->{'INST'};
#	$db->{'DB_NAME'}=$r->{'DB'};
}

sub Connect {
	my $conn = shift;
	my $tag = shift;
        my $dbh;

	if (not defined($conn->{'pass'})) {
		Logger::PrintMsg(Logger::ERROR, "$tag: no password given\n");
		return undef;
	}

	if (not defined($conn->{'port'})) {
                $conn->{'port'} = 1521;
        }

	Logger::PrintMsg(Logger::DEBUG, "$tag: dbi:Oracle://$conn->{host}:$conn->{port}/$conn->{service} user: $conn->{user}\n");
#  $dbh = DBI->connect('dbi:Oracle:host=foobar;sid=ORCL;port=1521;SERVER=POOLED', 'scott/tiger', '')
        $dbh = DBI->connect('dbi:Oracle://'.$conn->{'host'}.':'.$conn->{'port'}.'/'.$conn->{'service'}.':DEDICATED',
                            $conn->{'user'},
                            $conn->{'pass'});

	if (not defined($dbh)) { 
		Logger::PrintMsg(Logger::ERROR, "$DBI::errstr making connection \n");
		return undef;
	}

	SessionSetup($dbh);

        return $dbh;
}

1;
__END__
