#!bin/sh
table=$1
hive -e "create table $table(time varchar(8),userid varchar(30),query string,pagerank int,clickrank int,site string)partitioned by(year int,month int,day int)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'";
