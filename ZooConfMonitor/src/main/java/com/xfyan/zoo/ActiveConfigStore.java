package com.xfyan.zoo;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ActiveConfigStore extends ConnectWatcher {
	public void write(String path,File conf_zip) throws KeeperException, InterruptedException{
		Stat state = zk.exists(path, false);
		if(state == null){
			zk.create(path, file2byte(conf_zip), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}else{
			zk.setData(path, file2byte(conf_zip), -1);
		}
		
	}
	
	private byte[] file2byte(File file){

		byte[] bytes = null;
		
		try {
			FileInputStream fis = new FileInputStream(file);
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			byte[] b = new byte[1024];
			int len = 0;
			while((len = fis.read(b)) != -1){
				bos.write(b,0,len);
			}
			fis.close();
			bos.close();
			bytes = bos.toByteArray();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return bytes;
	}
	
	public File read(String path,Watcher watcher) throws KeeperException, InterruptedException{
		byte[] data = zk.getData(path, watcher, null);
		return byte2File(data);
	}
	
	private File byte2File(byte[] data){
		String path = System.getenv("HADOOP_HOME")+"/etc/hadoop/conf.zip";
		File file = new File(path);
		FileOutputStream fos = null;
		BufferedOutputStream bos = null;
		
		try {
			fos = new FileOutputStream(file);
			bos = new BufferedOutputStream(fos);
			bos.write(data);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				bos.close();
				fos.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return file;
	}
	
	
	
	
	
	
	
}
