package com.xfyan.movie;

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
	private KafkaStream stream;
	private int threadNumber;
	FileSystem hdfs;
	private String destDir;
	
	private FSDataOutputStream dos = null;
	private String outputFileName = null;
	private refreshThread refresher;
	
	public static Object syncObject = new Object();
	
	public SubTaskConsumer(KafkaStream stream, int threadNumber,FileSystem fs,String destDir) {
		threadNumber = threadNumber;
		stream = stream;
		hdfs = fs;
		this.destDir = destDir;
		init();
	}
	
	
	
	class refreshThread implements Runnable {
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
	
	private void init(){
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
		refresher = new refreshThread();
		scheduler.scheduleAtFixedRate(refresher, 0, 1000, TimeUnit.MILLISECONDS);
	}
	
	
	public void run() {
		try {
			String pathString = destDir;
			Path path = new Path(pathString);
			dos = hdfs.create(path);
			ConsumerIterator<byte[],byte[]> it = stream.iterator();
			while(it.hasNext()){
				MessageAndMetadata<byte[],byte[]> mem = it.next();
				byte[] message = mem.message();
				
				dos.write(message,0,message.length);
				dos.write("\n".getBytes());
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(dos != null){
				try {
					dos.close();
					dos.flush();
					hdfs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}


}
