#!/bin/bash
input_dir=$1
table=$2
while true;do
	#查看input_dir是否存在
	`hadoop fs -test -e $input_dir`
	if [ $? -eq 0 ];then
		#将所有文件路径存入数组中
		array=(`hadoop fs -ls -R $input_dir | awk '{if ($NF ~ /Done$/) print $NF}'`)
		#如果数组为空
		if [ ${#array[@]} -eq 0 ];then
			echo ${#array[@]}
			echo "loading hive..."
		else
			cd /data/search-query/Hdfs2Hive && sh load-to-hive.sh $input_dir $table
			
				
			sleep 100
		fi
	else
		echo" loading hive......"
	fi
done
