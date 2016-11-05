DayTableName=$1
MonthTableName=$2
WeekTableName=$3
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

