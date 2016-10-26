package com.xfyan.kafka2hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HadoopConsumer {
	 private final ConsumerConnector consumer;
	    private final String topic;
	    private  ExecutorService executor;
	    private FileSystem hdfs;
	    private String destDir;
	    
	    public HadoopConsumer(String a_zookeeper, String a_groupId, String a_topic,String destDir) {
	        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
	                createConsumerConfig(a_zookeeper, a_groupId));
	        this.topic = a_topic;
	        this.destDir=destDir;
	        try {
				hdfs = FileSystem.get(new Configuration());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	    }

	    public void shutdown() {
	        if (consumer != null) consumer.shutdown();
	        if (executor != null) executor.shutdown();
	    }

	    public void run(int a_numThreads) {
	        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	        topicCountMap.put(topic, new Integer(a_numThreads));
	        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
	        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

	        executor = Executors.newFixedThreadPool(a_numThreads);

	        int threadNumber = 0;
	        for (final KafkaStream stream : streams) {
	            executor.submit(new SubTaskConsumer(stream, threadNumber,hdfs,destDir));
	            threadNumber++;
	        }
	    }

	    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
	        Properties props = new Properties();
	        props.put("zookeeper.connect", a_zookeeper);
	        props.put("group.id", a_groupId);
	        props.put("zookeeper.session.timeout.ms", "60000");
	        props.put("zookeeper.sync.time.ms", "2000");
	        props.put("auto.commit.interval.ms", "1000");
	        props.put("auto.offset.reset","smallest");
	        return new ConsumerConfig(props);
	    }

	    public static void main(String[] args) throws Exception {
	    	//read five parameters:1 topic_name,2:group_name,3:zookeeper,4:hdfs dest dir,5:threads num from config file;
	    	String path=System.getProperty("user.dir");
	    	InputStream is=new BufferedInputStream(new FileInputStream(new File(path+"/consumer.properties")));
	    	Properties prop=new Properties();
	    	prop.load(is);
	    	
	    	String zooKeeper=prop.getProperty("zookeeper");
	    	String topic=prop.getProperty("topic");
	    	String groupId=prop.getProperty("groupId");
	    	String destDir=prop.getProperty("destDir");
	    	int threads=Integer.parseInt(prop.getProperty("threadNumber"));
	    	

	        HadoopConsumer example = new HadoopConsumer(zooKeeper, groupId, topic,destDir);
	        example.run(threads);
	    }
}
