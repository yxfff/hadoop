#!/bin/bash
DayTableName=$1
MonthTableName=$2
WeekTableName=$3
year=$4
month=$5
day=$6
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

