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


	echo "load data inpath '$i' into table $table partition(year=$year,month=$month,day=$day)"
	hive -e "load data inpath '$i' into table $table partition(year=$year,month=$month,day=$day)"

	#load to mysql
	week=`echo date +%w`
	WeekTableName="week"$prex$year$week
	
	cd /data/search-query/Hdfs2Mysql && create-mysql-table.sh $DayTableName $MonthTableName $WeekTableName

        cd /data/search-query/Hdfs2Mysql && sqoop-to-mysql.sh $DayTableName $MonthTableName $WeekTableName $year $month $day

done



