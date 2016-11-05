四台机器部署：
hadoop集群：xfyan 单机
flume agent：xfyan
kafka broker：binder3，binder4
zookeeper：binder2,binder3,binder4
mysql:xfyan



执行步骤
1.启动kafka brocker(binder3,binder4)
bin/kafka-server-start.sh config/server.properties &

创建topic
	bin/kafka-topics.sh --create --zookeeper binder2:2181 --replication-factor 2 --partitions 2 --topic xfyan_topic
	查看topic
	bin/kafka-topics.sh --describe --zookeeper binder2:2181 --topic xfyan_topic

2.启动kafka consumer
修改配置：vim consumer.properties

cd /data/search-query/KafkaToHdfs 
sh start-consumer.sh

3.启动flume agent

cd /data/search-query/Log2Kafka
sh flume-agent.sh

4.创建hive表
cd /data/search-query/Hdfs2Hive
sh create-table.sh  log_test

5.启动hive load程序
sh start-loading.sh /log/kafka/  log_test

6.产生数据
sh log-generator.sh 

