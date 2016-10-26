#!/bin/bash
input_dir=$1
table=$2
array=(`hadoop fs -ls -R $input_dir | awk '{if ($NF ~ /Done$/) print $NF}'`)
for i in ${array[@]};do
	year=`echo $i | awk -F "/" '{print $(NF-3)}'`
	month=`echo $i | awk -F "/" '{print $(NF-2)}'`
	day=`echo $i | awk -F "/" '{print $(NF-1)}'`

	prex="xfyan"
	DayTableName=$prex$year$month$day
	MonthTbaleName=$prex$year$month

	d=$year$month$day
	month=`echo $month | sed 's/^0*//'`
	day=`echo $day | sed 's/^0*//'`

	echo "load data inpath '$i' into table $table partition(year=$year,month=$month,day=$day)"
	hive -e "load data inpath '$i' into table $table partition(year=$year,month=$month,day=$day)"
	#load to mysql
	#week_str=`date -d $d + $w`
	#week=`echo $week_str | sed 's/^0*//'`
	#week=$(expr $week +1)
	#week=`printf "%02d\n" $w`
	#weekTableName="week"$prex$year$week
	
	week=`echo date +%w`
	WeekTableName="week"$prex$year$week
	
	mysql -hxfyan -uroot -p123456 -e "use log_test;
		create table if not exists $DayTableName(
		  time varchar(8),
		  userid varchar(30),
		  query varchar(40),
		  pagerank int,
		  clickrank int,
		  site varchar(100)
		);
		create table if not exists $MonthTableName(
		  time varchar(8),
		  userid varchar(30),
		  query varchar(40),
		  pagerank int,
		  clickrank int,
		  site varchar(100)
		);
		create table if not exists $WeekTableName(
		  time varchar(8),
		  userid varchar(30),
		  query varchar(40),
		  pagerank int,
		  clickrank int,
		  site varchar(100)
		);"		
	

	cd /usr/lib/sqoop && bin/sqoop export -Dsqoop.export.records.per.statement=20 \
		--connect jdbc:mysql://xfyan:3306/log_test \
		--username root \
		--password 123456 \
		--table $DayTableName \
		--export-dir /user/hive/warehouse/bootcamp/hivetest/year=$year/month=$month/day=$day \
		--fields-terminated-by '\t'

	cd /usr/lib/sqoop && bin/sqoop export -Dsqoop.export.records.per.statement=20 \
		--connect jdbc:mysql://xfyan:3306/log_test \
		--username root \
		--password 123456 \
		--table $MonthTableName \
		--export-dir /user/hive/warehouse/bootcamp/hivetest/year=$year/month=$month/day=$day \
		--fields-terminated-by '\t'

	cd /usr/lib/sqoop && bin/sqoop export -Dsqoop.export.records.per.statement=20 \
		--connect jdbc:mysql://xfyan:3306/log_test \
		--username root \
		--password 123456 \
		--table $WeekTableName \
		--export-dir /user/hive/warehouse/bootcamp/hivetest/year=$year/month=$month/day=$day \
		--fields-terminated-by '\t'

done



