package com.xfyan.zoo;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.zookeeper.KeeperException;

public class HadoopConfigUpdater {
	public static final String path = "/config";
	public String config_file_path;
	public String conf_zip_path;
	
	private ActiveConfigStore store;
	
	public HadoopConfigUpdater(String hosts) throws IOException {
		store = new ActiveConfigStore();
		store.connect(hosts);
		String hadoop_home = System.getenv("HADOOP_HOME");
		config_file_path = hadoop_home + "/etc/hadoop";
		conf_zip_path=hadoop_home+"conf.zip";
	}
	
	public void run() throws IOException, KeeperException, InterruptedException{
		zipFile(config_file_path,conf_zip_path);
		store.write(path, new File(conf_zip_path));
	}
	
	private void zipFile(String conf_file_path,String conf_zip_path) throws IOException{
		List<File> fileList = getSubFiles(new File(conf_file_path));
		ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(conf_zip_path));
		byte[] buffer = new byte[1024];
		ZipEntry ze = null;
		int readLen = 0;
		for(int i = 0; i < fileList.size();i++){
			File f = (File) fileList.get(i);
			ze = new ZipEntry(f.getName());
			ze.setSize(f.length());
			zos.putNextEntry(ze);
			InputStream is = new BufferedInputStream(new FileInputStream(f));
			while((readLen = is.read(buffer, 0, 1024)) != -1){
				zos.write(buffer,0,readLen);
			}
			
			is.close();
		}
		
		zos.close();
		
		
		
		
		
		
		
		
		
		
	}
	
	private List<File> getSubFiles(File dir){

		List<File> files = new ArrayList<File>();
		File[] tmp = dir.listFiles();
		for(int i = 0; i < tmp.length;i++){
			if(tmp[i].isFile()){
				files.add(tmp[i]);
			}
		}
		return files;
	}
	
	
	public static void main(String[] args) throws Exception {
		HadoopConfigUpdater updater = new HadoopConfigUpdater(args[0]);
		updater.run();
	}
}
