package com.xfyan.kafka2hdfs;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SubTaskConsumer implements Runnable {
	private KafkaStream m_stream;
	private int m_threadNumber;
	FileSystem hdfs;
	private String destDir;

	private FSDataOutputStream dos=null;
	private String outputFileName=null;
	private refreshThread refresher;

	public static Object syncObj=new Object();

	public SubTaskConsumer(KafkaStream a_stream, int a_threadNumber,FileSystem fs, String destDir) {
		m_threadNumber = a_threadNumber;
		m_stream = a_stream;
		hdfs = fs;
		this.destDir=destDir;
		init();
	}

	public void run() {
		try {		
			ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
			System.out.println(it.toString());
			while (it.hasNext()){
				MessageAndMetadata<byte[], byte[]> mem= it.next();
				byte[] message=mem.message();//data bytes
				byte[] decodedKeyBytes=mem.key(); //filename bytes
				String dataString="20150101";   //default data value;   //data date
				System.out.println(new String(message));
				if(decodedKeyBytes!=null){
					String sourceFileName=new String(decodedKeyBytes);
					int LastDotIndex=sourceFileName.lastIndexOf(".");
					dataString=sourceFileName.substring(LastDotIndex+1);
				}
				dumpToFile(dataString,message);	        	
			}
		} catch (IOException e) {
			e.printStackTrace();			
		} finally{
			System.out.println("shutting down the thread:"+this.m_threadNumber);
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
		String year=dataString.substring(0, 4);
		String month=dataString.substring(4, 6);
		String day=dataString.substring(6);
		String pathString=destDir+"/"+year+"/"+month+"/"+day+"/kafkaData"+this.m_threadNumber;
		Path path=new Path(pathString);
		System.out.println(pathString+"    "+outputFileName);
		if(outputFileName==null || dos==null || !pathString.equals(outputFileName)){
			if(dos!=null){
				synchronized(syncObj){
					dos.flush();
					dos.close();
				}
				String name=outputFileName+"."+System.currentTimeMillis()+".Done";
				hdfs.rename(new Path(outputFileName),new Path(name));
				System.out.println("rename the log file's name: "+outputFileName+" ===>"+name);
			}
			if(hdfs.exists(path)){
				synchronized(syncObj){
					dos=hdfs.append(path);
				}
				System.out.println("open the output in the append mode!");
			}else{
				synchronized(syncObj){
					dos=hdfs.create(path);
				}
				System.out.println("create the new output file!");
			}		
			outputFileName=pathString;
		}
		dos.write(message,0,message.length);
		dos.write("\n".getBytes());
	}

	class refreshThread implements Runnable{
		public void run() {
			if(dos!=null){
				try {
					dos.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}	
	}

	private void init(){
		ScheduledExecutorService scheduler=Executors.newSingleThreadScheduledExecutor();
		refresher=new refreshThread();
		scheduler.scheduleAtFixedRate(refresher, 0, 1000, TimeUnit.MILLISECONDS);
	}
}
