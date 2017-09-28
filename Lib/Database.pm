package Lib::Database;

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
use threads;
use threads::shared;

use Lib::Logger qw(DEBUG DEBUG2 ERROR PrintMsg);


#GetColumns
use constant CHECK_COLUMN_TYPE => 1;
use constant CHECK_COLUMN_NULLABLE => 2;

#GetPrimaryKey
use constant PK_CHECK_ONLY => 1;
use constant PK_DONT_CHECK => 2;

#comparison methods
use constant COMPARE_USING_PK => 0;
use constant COMPARE_USING_COLUMN => 1;
use constant COMPARE_USING_SHA1 => 2;

#
use constant RESULT_BATCH_SIZE => 10000;

use base 'Exporter';
our @EXPORT_OK = qw(CHECK_COLUMN_TYPE CHECK_COLUMN_NULLABLE PK_CHECK_ONLY PK_DONT_CHECK COMPARE_USING_PK COMPARE_USING_COLUMN COMPARE_USING_SHA1 RESULT_BATCH_SIZE);

#
sub PreparePartitionLimits {

$cols = ['VND_CD', 'FLT_NBR','SVC_STR_CTY_CD'];
$rows = [['AA','2295','MIA'],['AA','2584','DFW'],['AM','0414','MTY'],['AM','0472','MEX']];
#print Dumper(\@cols);
#print Dumper(\@rows);
$r_lo = undef;
$r_hi = undef;
for ($k=0;$k<=scalar(@{$rows});$k++) {
        $r_hi = $rows->[$k] if ($k < scalar(@{$rows}));
        print "LOW: ".join(',',@{$r_lo}),"\n" if ($r_lo);
        print "HIGH: ".join(',',@{$r_hi}),"\n" if ($r_hi);

        $s='';
        if ($r_lo) {
                $s.='(';
                for ($i = 0; $i < scalar(@{$cols}); $i++) {
                        $s.=') OR (' if ($i > 0);
                        for ($j = 0; $j <= $i; $j++) {
                                $s.=' AND ' if ($j > 0);
                                if ($i==$j) {
                                        if ($i == scalar(@{$cols}) - 1) {
                                                $s .= $cols->[$j]." >= '".$r_lo->[$j]."'";
                                        } else {
                                                $s .= $cols->[$j]." > '".$r_lo->[$j]."'";
                                        }
                                } else {
                                        $s .= $cols->[$j]." = '".$r_lo->[$j]."'";
                                }
                        }
                }
                $s.=')';
        }
        $s.="\nAND\n" if ($r_hi and $r_lo);
        if ($r_hi) {
                $s.='(';
                for ($i = 0; $i < scalar(@{$cols}); $i++) {
                        $s.=') OR (' if ($i > 0);
                        for ($j = 0; $j <= $i; $j++) {
                                $s.=' AND ' if ($j > 0);
                                if ($i==$j) {
                                        $s .= $cols->[$j]." < '".$r_hi->[$j]."'";
                                } else {
                                        $s .= $cols->[$j]." = '".$r_hi->[$j]."'";
                                }
                        }
                }
                $s.=')';
        }
        print $s,"\n";
        print "-------------\n";
        $r_lo = $r_hi;
        $r_hi = undef;
}

}

sub GetLogicalPartitions {
	PreparePartitionLimits();	
}

# when modifing %{$columns} - $action=PK_DONT_CHECK - run in critical section!
sub GetPrimaryKey {
	my $columns = shift; #reference for 'columns' hash
	my $dbh = shift; #db handler
	my $object = shift; #reference to 'object' hash
	my $tag = shift; #tag for debug messages
	my $action = shift; #PK_CHECK_ONLY -> compare retrieved values to stored in %{$columns}
			    #PK_DONT_CHECK -> retrieve values and store in %{$columns}

	my $owner;
	my $table;
	my $sql;

	$owner = $object->{owner} if($object->{owner});
	$table = $object->{table};
	$tag = '' if (!defined($tag));

	$sql = "SELECT cols.table_name, cols.column_name, cons.status, cons.owner,cons.constraint_type ctype,cols.constraint_name,cols.position ";
	$sql .= "FROM all_constraints cons join all_cons_columns cols on (cols.constraint_name=cons.constraint_name) WHERE ";
	$sql .= "cons.constraint_type in ('P','U') AND cons.owner = cols.owner AND cons.status = 'ENABLED' and cols.table_name='$table' ";
	$sql .= " and cols.owner='$owner'" if ($owner);
	$sql .= "order by ctype, constraint_name, position";

	PrintMsg(DEBUG, $tag, $sql);

	my $r = $dbh->selectall_arrayref($sql, { Slice => {} } );

	my $pk_found = 0;
	my $u_found = 0;
	my $cname;
	
	if ($action == PK_CHECK_ONLY) { #pk/uniq was retrieved by some other worker, so we will compare 

		foreach my $row (@{$r}) {

			if ($row->{CTYPE} eq 'P') { #constraint is PK
				$pk_found = 1;
				$cname = $row->{CONSTRAINT_NAME};

				if (($columns->{$row->{COLUMN_NAME}}->{CONSTRAINT} ne 'P') #compare if stored column is in PK
				 #   or ($COLUMNS{$row->{COLUMN_NAME}}->{CPOSITON} != $row->{POSITION}) #compare if stored column is in the same position
				 #   or ($COLUMNS{$row->{COLUMN_NAME}}->{CONSTRAINT_NAME} ne $row->{CONSTRAINT_NAME}) #compare if PK's name is the same
				) {
					PrintMsg(ERROR, $tag, "$table/$row->{COLUMN_NAME} PK constraint differs");
					return -1;
				}

			} elsif ($pk_found == 0 and $row->{CTYPE} eq 'U') { #there was no PK captured before but we have UK. That will do.

				if ($u_found == 0 or $cname eq $row->{CONSTRAINT_NAME} ) {

					if (($columns->{$row->{COLUMN_NAME}}->{CONSTRAINT} ne 'U') #compare if column is in Uniq
					#    or ($COLUMNS{$row->{COLUMN_NAME}}->{CPOSITON} != $row->{POSITION}) #compare position in constraint
					#    or ($COLUMNS{$row->{COLUMN_NAME}}->{CONSTRAINT_NAME} ne $row->{CONSTRAINT_NAME}) #compare constraint name
					) {
						PrintMsg(ERROR, $tag, "$table/$row->{COLUMN_NAME} Uniqe constraint differs");
						return -1;
					}

					$cname = $row->{CONSTRAINT_NAME};
					$u_found = 1;
				}
			}
		}

	} else { #save pk/uniq columns for later comparision

		foreach my $row (@{$r}) {

			if ($row->{CTYPE} eq 'P') {
				$pk_found = 1;

				$columns->{$row->{COLUMN_NAME}}->{CONSTRAINT}='P';
				$columns->{$row->{COLUMN_NAME}}->{CPOSITON}=$row->{POSITION};
				$columns->{$row->{COLUMN_NAME}}->{CONSTRAINT_NAME}=$row->{CONSTRAINT_NAME};
				$cname = $row->{CONSTRAINT_NAME};
			} elsif ($pk_found == 0 and $row->{CTYPE} eq 'U') { #there is "order by ctype" so P will be always before U
				#there was no PK captured before but we have Uniq
				if ($u_found == 0 or $cname eq $row->{CONSTRAINT_NAME} ) {
					$u_found = 1;

					$columns->{$row->{COLUMN_NAME}}->{CONSTRAINT}='U';
					$columns->{$row->{COLUMN_NAME}}->{CPOSITON}=$row->{POSITION};
					$columns->{$row->{COLUMN_NAME}}->{CONSTRAINT_NAME}=$row->{CONSTRAINT_NAME};
					$cname = $row->{CONSTRAINT_NAME};
				}
			}
		}

	}


	if ($u_found + $pk_found == 0) { #neither PK nor Uniqe
		PrintMsg(ERROR, $tag, "There is neither PK nor Unique constraint enabled for table $table.");
		return -1;
	}


	if ($action == PK_CHECK_ONLY) { 

		PrintMsg(DEBUG, $tag, "Constraint PK/U verified correctly");

	} else { #this is first worker to retrieve PK/UK information

		my $str = "Columns for constraint $cname: ";
		my @cols = sort { $columns->{$a}->{CPOSITON} <=> $columns->{$b}->{CPOSITON} } grep {defined $columns->{$_}->{CPOSITON}} keys %{$columns};
		$str .= join(',', @cols);
		PrintMsg(DEBUG, $tag, "$str $cname");

	}

	return 0;
}

# when populating %{$columns} run in critical section!
sub GetColumns {
	my $columns = shift; #reference for 'columns' hash
	my $dbh = shift; #db handler
	my $object = shift; #reference to 'object' hash
	my $tag = shift; #tag for debug messages
	my $checks = shift; #run following checks on column list

	my $owner;
	my $table;
	my $sql;

	my ($cn, $c);

	$owner = $object->{owner} if($object->{owner});
	$table = $object->{table};
	$tag = '' if (!defined($tag));

	$sql = "select OWNER,TABLE_NAME,COLUMN_NAME,DATA_TYPE,NULLABLE,COLUMN_ID from ";
	$sql .= "all_tab_columns where table_name='$table'";
	$sql .= " and owner='$owner'" if ($owner);

	PrintMsg(DEBUG, $sql);

	my $r = $dbh->selectall_hashref($sql, 'COLUMN_ID');

	if ( scalar(keys(%{$r})) == 0 ) {
		PrintMsg(ERROR, $tag, "Cannot get columns for $table, do I have proper grants?\n");
		return undef;
	}

	if (scalar keys %{$columns}) { #is it populated already? 

		# if there are columns retrieved already then just compare if the match
		foreach $c (keys %{$r}) { 
			$cn = $r->{$c}->{'COLUMN_NAME'};

			if ( $checks > 0  && (not defined($columns->{$cn})) ) {
				PrintMsg(ERROR, $tag, "Different tables, unexpected $cn column");
				return -128;
			}

			if ( ($checks & CHECK_COLUMN_TYPE) &&
			     ($columns->{$cn}->{'DATA_TYPE'} ne $r->{$c}->{'DATA_TYPE'}) ) {

				PrintMsg(ERROR, $tag, "Data types for column $cn differs");
				return -1 * CHECK_COLUMN_TYPE;
			}

			my $n = 'N';
			$n = 'Y' if (defined $columns->{$cn}->{'NULLABLE'});
			if ( ($checks & CHECK_COLUMN_NULLABLE) && $r->{$c}->{'NULLABLE'} ne $n) {

				PrintMsg(ERROR, $tag, "NULLABLE flag is inconsistnt for column $cn");
				return -1 * CHECK_COLUMN_NULLABLE;
			}

		}

		if ( (scalar keys %{$r}) ne (scalar keys %{$columns}) ) {
			PrintMsg(ERROR, $tag, "Different tables.");
                        return -128;
		}

		PrintMsg(DEBUG, $tag, "Columns verified correctly");

		return GetPrimaryKey($columns, $dbh, $object, $tag, PK_CHECK_ONLY);

	} else {
		# read column list and store to shared hash ref.
		foreach $c (keys %{$r}) {
			$cn = $r->{$c}->{'COLUMN_NAME'};

			if (is_shared($columns)) {
				$columns->{$cn} = &share({});
				PrintMsg(DEBUG2, $tag, "Sharing $cn hash ref.");
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

		PrintMsg(DEBUG, $tag, $s);

		return GetPrimaryKey($columns, $dbh, $object, $tag, PK_DONT_CHECK);
	}

}

sub PrepareFirstStageSelect {
	my $object = shift;
	my $columns = shift;
	my $settings = shift;

	my $cmp_method = $settings->{cmp_method};
	my $parallel = $settings->{select_concurency};
	
	#get pk columns
	my @pk = sort { $columns->{$a}->{CPOSITON} <=> $columns->{$b}->{CPOSITON} } grep {defined $columns->{$_}->{CPOSITON}} keys %{$columns};

	#map excluded columns from array to hash
	my %excl_col = map {$_=>1} @{$settings->{exclude_cols}}; #map from array to hash

	
	my $tablename = $object->{table};
        my $schema = $object->{owner};
	my $partition = '';
	my $range = '';

	$partition = $object->{partition_name} if (defined($object->{partition_name}));
	$partition = $object->{partition_for} if (defined($object->{partition_for}));
	$range = $object->{pk_range} if (defined($object->{pk_range}));

	my $sql = 'SELECT ';

        if ($cmp_method == COMPARE_USING_COLUMN) {

                $sql .= join(',', @pk).','.$settings->{compare_col};

        } elsif ($cmp_method == COMPARE_USING_SHA1) {

		my $separator='';
		$sql .=  " /*+ PARALLEL($parallel) */ " if ($parallel);
		$sql .= join(',', @pk).", DBMS_CRYPTO.Hash(\n";

	        foreach my $i (sort { $columns->{$a}->{'COLUMN_ID'} <=> $columns->{$b}->{'COLUMN_ID'} } keys %{$columns}) {

			#don't include this column in SHA1 if it is part of constraint PK/UK
			next if (defined($columns->{$i}->{CPOSITON})); 

			next if (defined($excl_col{$i})); #check if we want to skip this column

	                if ($columns->{$i}->{DATA_TYPE} eq 'BLOB' || $columns->{$i}->{DATA_TYPE} eq 'CLOB' ) {
				if (defined $columns->{$i}->{NULLABLE}) {
	        	                $sql .= $separator."DBMS_CRYPTO.Hash(NVL($i,'00'),3)\n"; #NVL(some data, some hex as alternative);
				} else {
		                        $sql .= $separator."DBMS_CRYPTO.Hash($i,3)\n";
				}
	                } elsif ( $columns->{$i}->{DATA_TYPE} eq 'XMLTYPE') {
				if (defined $columns->{$i}->{NULLABLE}) {
					#$sql .= $separator."DBMS_CRYPTO.Hash(NVL(XMLType.getBlobVal($i,nls_charset_id('AL32UTF8')),'00'),3)\n"; 
	                                # with getBlobVal parallel doesn't work, but hash is working strange way
	                                $sql .= $separator."DBMS_CRYPTO.Hash(NVL2($i,XMLType.getClobVal($i),'00'),3)\n";
				} else {
		                        #$sql .= $separator."DBMS_CRYPTO.Hash(XMLType.getBlobVal($i,nls_charset_id('AL32UTF8'),3)\n";
					$sql .= $separator."DBMS_CRYPTO.Hash(XMLType.getClobVal($i),3)\n";
				}
	                } else {
				if (defined $columns->{$i}->{NULLABLE}) {
	                        	$sql .= $separator."DBMS_CRYPTO.Hash(NVL(utl_raw.cast_to_raw($i),'00'),3)\n";
				} else {
		                        $sql .= $separator."DBMS_CRYPTO.Hash(utl_raw.cast_to_raw($i),3)\n";
				}
	                }
	                $separator='||';
	        }       
		$sql .= ",3)";

        } else { #COMPARE_USING_PK

                $sql .= join(',', @pk).", 1";

        }

	$sql .= " CMP#VALUE FROM $schema.$tablename $partition WHERE $range ORDER BY ".join(',', @pk);


	return $sql;
}

sub PrepareSecondStageSelect {
	my $object = shift;
	my $columns = shift;
	my $settings = shift;

	my $cmp_method = $settings->{cmp_method};
	my $tablename = $object->{table};
        my $schema = $object->{owner};
	my $partition = '';
	my $range = '1=1';

	my $sql = 'SELECT ';

	#get pk columns
	my @pk = sort { $columns->{$a}->{CPOSITON} <=> $columns->{$b}->{CPOSITON} } grep {defined $columns->{$_}->{CPOSITON}} keys %{$columns};

	$partition = $object->{partition_name} if (defined($object->{partition_name}));
	$partition = $object->{partition_for} if (defined($object->{partition_for}));
	$range = $object->{pk_range} if (defined($object->{pk_range}));



        if ($cmp_method == COMPARE_USING_COLUMN) {

		$sql = 'SELECT '.$settings->{'compare_col'}." FROM $tablename WHERE ".join(" and ", map { "$_=?" } @pk ).' AND '.$range;

	} elsif ($cmp_method == COMPARE_USING_SHA1) {

		$sql = SHA1Sql(@pk).$tablename.' WHERE '.join(" and ", map { "$_=?" } @pk );

	} else { #default COMPARE_USING_PK

		$sql = "SELECT 'exists' FROM $tablename WHERE ".join(" and ", map { "$_=?" } @pk ).' AND '.$range;
	}

	$sql .= " CMP#VALUE FROM $schema.$tablename $partition WHERE $range ORDER BY ".join(',', @pk);

	return $sql;
}

sub SessionSetup {
	my $dbh = shift;

	$dbh->{RaiseError} = 0; #dont die imediately
        $dbh->{RowCacheSize} = RESULT_BATCH_SIZE; 

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
		PrintMsg(ERROR, "$tag: no password given\n");
		return undef;
	}

	if (not defined($conn->{'port'})) {
                $conn->{'port'} = 1521;
        }

	PrintMsg(DEBUG, "$tag: dbi:Oracle://$conn->{host}:$conn->{port}/$conn->{service} user: $conn->{user}\n");
	#  $dbh = DBI->connect('dbi:Oracle:host=foobar;sid=ORCL;port=1521;SERVER=POOLED', 'scott/tiger', '')
        $dbh = DBI->connect('dbi:Oracle://'.$conn->{'host'}.':'.$conn->{'port'}.'/'.$conn->{'service'}.':DEDICATED',
                            $conn->{'user'},
                            $conn->{'pass'});

	if (not defined($dbh)) { 
		PrintMsg(ERROR, "$DBI::errstr making connection \n");
		return undef;
	}

	SessionSetup($dbh);

        return $dbh;
}

1;
__END__
