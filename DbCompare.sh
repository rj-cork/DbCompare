#!/bin/bash

DIR=~/CMP
DBNAME=TESTDB

DB0="user/pass@rac1/TESTDB1"
DB1="user/pass@rac2/TESTDB2"
SCHEMA=DATA_OWNER
TIME="Mo-Su 22:00-6:00"
MAXLOAD=20
PARALLEL=4
EXCLUDE=${SCHEMA}.NONREPLICATED_TABLE
STATEFILE=DbCompare.${DBNAME}.state
PIDFILE=DbCompare.${DBNAME}.pid
LOGFILE=DbCompare.${DBNAME}.log
MAXLOG=14
OUTPUT=out_$DBNAME.log
EMAIL=some.addres@somewhere.com
OUTPUTLINES=1000 #max 1000 lines is send by email
CONTINUE=""

if [ "$1" == "CONTINUE" ]; then
	CONTINUE="--continueonly"
fi

export ORACLE_HOME=/u01/app/11.2.0.3/grid
export LD_LIBRARY_PATH=$ORACLE_HOME/lib
export PATH=/bin:/usr/bin


cd $DIR || exit 1

[ -e "$LOGFILE.$MAXLOG" ] && rm -v $LOGFILE.$MAXLOG
for ((i=$MAXLOG-1;i>0;i--)); do 
	[ -e "$LOGFILE.$i" ] && mv -v $LOGFILE.$i $LOGFILE.$(($i+1))
done
mv -v $LOGFILE $LOGFILE.1

echo "==========================================" >> $OUTPUT
echo "Started at "`date` >> $OUTPUT
perl DbCompare.pl $CONTINUE\
 --db $DB0\
 --db $DB1\
 --schema $SCHEMA\
 --range $TIME\
 --logfile $LOGFILE\
 --state $STATEFILE\
 --maxload $MAXLOAD\
 --pidfile $PIDFILE\
 --parallel $PARALLEL\
 --exclude $EXCLUDE >> $OUTPUT 2>&1
echo "Stopped at "`date` >> $OUTPUT

[ -e $LOGFILE ] && grep -q "Final results" $LOGFILE && (grep -A${OUTPUTLINES} "Final results" $LOGFILE | mail -s "'$DBNAME report'" $EMAIL)


