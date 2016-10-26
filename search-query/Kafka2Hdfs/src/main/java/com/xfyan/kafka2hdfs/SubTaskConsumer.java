package com.xfyan.kafka2hdfs;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class SubTaskConsumer implements Runnable {
	private KafkaStream<byte[], byte[]> stream;
	private int threadNumber;
	FileSystem hdfs;
	private String destDir;
	private FSDataOutputStream dos =null;
	private String outputFileName = null;
	private refreshThread refresher;
	
	public static Object syncObject = new Object();
	
	public SubTaskConsumer(KafkaStream<byte[], byte[]> stream,int threadNumber,FileSystem fs,String destDir) {
		this.threadNumber = threadNumber;
		this.stream = stream;
		this.hdfs = fs;
		this.destDir = destDir;
		init();
	}
	
	class refreshThread implements Runnable{

		public void run() {
			if(dos != null){
				try {
					dos.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	private void init() {
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		refresher = new refreshThread();
		scheduler.scheduleAtFixedRate(refresher, 0, 1000, TimeUnit.MILLISECONDS);
	}
	
	
	
	
	public void run() {
		try {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			System.out.println(it.toString());
			while(it.hasNext()){
				MessageAndMetadata<byte[], byte[]> mem = it.next();
				byte[] message = mem.message();
				byte[] decodeKeyBytes = mem.key();
				String dataString = "20150101";
				System.out.println(new String(message));
				
				if(decodeKeyBytes != null){
					String sourceFileName = new String(decodeKeyBytes);
					int LastDotIndex = sourceFileName.lastIndexOf(".");
					dataString = sourceFileName.substring(LastDotIndex+1);
				}
				
				dumpToFile(dataString,message);
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			System.out.println("shut down the thread:"+ this.threadNumber);
			
			try {
				if(dos!=null){
					dos.flush();
					dos.close();
				}
				hdfs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}




	private void dumpToFile(String dataString, byte[] message) throws IOException {
		String year = dataString.substring(0,4);
		String month = dataString.substring(4,6);
		String day = dataString.substring(6);
		String pathString = destDir + "/" + year +"/"+month+"/"+day+"/kafkaData"+this.threadNumber;
		Path path = new Path(pathString);
		System.out.println(pathString + "-"+outputFileName);
		
		if(outputFileName == null || dos == null|| !pathString.equals(outputFileName)){
			if(dos != null){
				synchronized(syncObject){
					dos.flush();
					dos.close();
				}
				String name = outputFileName + "."+System.currentTimeMillis()+".Done";
				hdfs.rename(new Path(outputFileName), new Path(name));
				System.out.println("rename the file name---"+ outputFileName +"-->"+name);
			}
			if(hdfs.exists(path)){
				synchronized (syncObject) {
					dos = hdfs.append(path);
				}
			}else{
				synchronized (syncObject) {
					dos = hdfs.create(path);
				}
				System.out.println("create the new output file");
			}
			
			outputFileName = pathString;
		}
		
		dos.write(message,0,message.length);
		dos.write("\n".getBytes());
	}
}
