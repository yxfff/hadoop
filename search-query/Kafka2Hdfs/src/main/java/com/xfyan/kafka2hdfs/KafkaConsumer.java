package com.xfyan.kafka2hdfs;


import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer {
	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;
	private FileSystem hdfs;
	private String destDir;
	
	public KafkaConsumer(String zookeeper,String groupId,String topic,String destDir) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper,groupId));
		this.topic = topic;
		this.destDir = destDir;
		
		try {
			hdfs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void run(int a_numThreads){
		Map<String,Integer> topicCountMap = new HashMap<String,Integer>();
		topicCountMap.put(topic, new Integer(a_numThreads));
		Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[],byte[]>> streams = consumerMap.get(topic);
		
		executor = Executors.newFixedThreadPool(a_numThreads);
		
		int threadNumber = 0;
		for(KafkaStream<byte[], byte[]> stream:streams){
			executor.submit(new SubTaskConsumer(stream,threadNumber,hdfs,destDir));
			threadNumber++;
		}
	}
	
	
	public void shutdown(){
		if(consumer != null)consumer.shutdown();
		if(executor != null)executor.shutdown();
	}
	private ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
		Properties prop = new Properties();
		prop.put("zookeeper.connect", zookeeper);
		prop.put("group.id", groupId);
		prop.put("zookeeper.session.timeout.ms", "60000");
		prop.put("zookeeper.sync.time.ms", "2000");
		prop.put("auto.commit.interval.ms", "1000");
		prop.put("auto.offset.reset", "smallest");
		
		
		return new ConsumerConfig(prop);
	}
	
	public static void main(String[] args) {
		//args:1.topic name,2.group_name,3.zookeeper,4.hdfs dest dir, 5.threads num
		String path = System.getProperty("user.dir");
		try {
			InputStream is = new BufferedInputStream(new FileInputStream(new File(path+"/consumer.properties")));
			Properties prop = new Properties();
			prop.load(is);
			String zookeeper = prop.getProperty("zookeeper");
			String topic = prop.getProperty("topic");
			String groupId = prop.getProperty("groupID");
			String destDir = prop.getProperty("destDir");
			int threads = Integer.parseInt(prop.getProperty("threadNumber"));
			
			KafkaConsumer example = new KafkaConsumer(zookeeper, groupId, topic, destDir);
			example.run(threads);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
}
