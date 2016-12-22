#!/usr/bin/perl -w

# DataCompare.pl - script and manual for comparing data in multiple tables/table partitions
# Version 1.00
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
use Cwd;
use File::Basename;
use lib (dirname(Cwd::abs_path($0)));
use DataCompare;

DataCompare::DataCompare();


__END__

=head1 NAME

=over 4
 
 DataCompare.pl - Script for comparing data in tables across many databases.

=back

=head1 SYNOPSIS

DataCompare.pl --db user[/pass]@host1[:port]/service --db user[/pass]@host2[:port]/service --table schema.table [--compare LAST_UPDT_TM] [--keyonly] [--parallel NUMBER] [-v] [--logfile /file/to/log.txt] [--rounds NUMBER] [--sleep NUMBER] [--map dbalias=newtablename] [--partition NAME] [--partitionfor STATEMENT] [--help|-h]

=head1 DESCRIPTION

DataCompare.pl connects to multiple databases and compares given table/partition using column with last update timestamp if given or record's SHA1 hash sum.

=head2 Options

=over 4

=item -d, --db [dbalias=]user[/pass]@host[:port]/service 

Provides user credentials and connect string to monitored instance. You can connect to multiple databases, but you need at least two connections. You can assign names (dbalias) for those connections so you can refer to them in mapping options,

=item -t, --table [SCHEMA.]TABLENAME

Compares rows in given table. Can be provided with schema or without.

=item -m, --map dbalias=[NEWSCHEMA.]NEWTABLE

For given connection referred by dbalias set different schema name and different table name. You can use multiple --map options.

=item -c, --compare COLUMN

Compares rows by given column. It should be timestamp column and it should be updated each time a DML modifies the row. It is much faster than comparing whole row.

=item --keyonly

Compares rows using PK/UK only. It checks if given key exists in other databases without any checksums.

=item -p, --parallel NUMBER

Choose level of parallelism for query calculating SHA1 sums. Defaults to 4.

=item -r, --rounds NUMBER

After first stage comparision the script goes through recorded rows that were different and checks in all databases what are current values. The parameter defines max number of such passes. Defaults to 5.

=item -s, --sleep NUMBER

Number of seconds between comparision passes. Used i.e. for replication to catch up. Defaults to 30 seconds.

=item --partition NAME

Narrows data comparison to given partition. All compared table should have the same number of partitions by PK. Example: --partition P_0032

=item --partitionfor "partition for statement"

Narrows data comparison to given partition. All compared table should have the same number of partitions by PK. Example: --partitionfor "to_date('15-AUG-2007','dd-mon-yyyy')"

=item --mappartition dbalias=NEWPARTITION[,NEWPARTITION]

Changes partition name given by --partition to different partition in database referred as dbalias. If you add more partitions separated by comma, rows will be selected in first stage comparison using UNION clause for each NEWPARTITION. That can be useful if in one database you have data in one partition and in other the same data is split into number of other partitions. For example in db1 database table prd_ref has 32 hash partitions and in db2 database it has 128 partitions then for comparing partition no. 32 in db1 you can use --partition P_032 --mappartition db2=P_032,P_064,P_096,P_128

=item -v, --verbose

Increases verbosity level. You may add multiple -v to increase the level.

=item -l, --logfile FILENAME

Stores all output messages to given file. The file is opened in APPEND mode.

=item --nochecktype

Turns off comparison of column's type

=item --nochecknullable

Turns off comparison of column's NULLable flag.

=item -h, --help

Displays this message.

=back

=head1 EXAMPLES

DataCompare.pl --db db1=system/pass@rac1-scan/testdb1 --db db2=system/pass@rac2-scan/testdb2  --table=data_owner.table --compare=COLUMN_UPDT_TM --map db2=data_owner.table --partition=P_0001 --mappartition db2=P_0001,P_0032

DataCompare.pl --db system@rac1-scan/testdb1 --db system@rac2-scan/testdb2  --table=data_owner.othertable --partition=P_0005 

DataCompare.pl --db system@rac1-scan/testdb1 --db db2=system@rac2-scan/testdb2  --table=DATA_OWNER.table2 --map db2=schema_test.table2

DataCompare.pl --db db1=user@rac1-scan/testdb1 --db db2=user@rac2-scan/testdb2 --table=data_owner.table3 --partition=P_0004 --parallel=6 --logfile table3.P_0004.log

=head1 REPORTING BUGS

 Report bugs to rj.cork@gmail.com

