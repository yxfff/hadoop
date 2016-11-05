环境搭建：
hadoop集群：xfyan（单机）
kafka broker（binder3，binder4）
hbase:binder3(单机）


1.启动kafka broker

在binder3,binder4机器上执行

cd /MovieProject/kafkatohdfs
sh start-kafka-broker.sh

2.启动consumer

在binder4机器上执行

cd /MovieProject/kafkatohdfs
sh start-consumer.sh

3.启动flume agent

在xfyan机器上执行：

cd /MovieProject/datatokafka
sh flume-agent.sh

4.将导入到hdfs上的数据进行join操作

cd /data/MovieProject/hdfstohbase

sh start-join.sh

5.创建hbase表，并启动Mapreduce程序将join后的数据导入到hbase中

cd /data/MovieProject/hdfstohbase

sh create-table.sh

sh load-to-hbase.sh

6.使用mahout对数据集进行推荐

cd /data/MovieProject/mahout

sh movie_recommend.sh


