package com.xfyan.zoo;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class HadoopConfigWatcher implements Watcher{
	private ActiveConfigStore store;
	public String conf_file_path;
	
	public HadoopConfigWatcher(String hosts) throws IOException {
		store = new ActiveConfigStore();
		store.connect(hosts);
		conf_file_path = System.getenv("HADOOP_HOME")+"/etc/hadoop/";
	}

	public void process(WatchedEvent event) {
		System.out.println("process " + event + " event!");
		if(event.getType() == Event.EventType.NodeDataChanged){
			try {
				handleConfig();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void handleConfig() throws KeeperException, InterruptedException{
		File config_zip = store.read(HadoopConfigUpdater.path,this);
		unzipFile(config_zip);
	}
	
	private void unzipFile(File config_zip){
		try {
			ZipFile zfile = new ZipFile(config_zip.getAbsolutePath().toString());
			Enumeration zlist = zfile.entries();
			byte[] buffer = new byte[1024];
			ZipEntry ze = null;
			while(zlist.hasMoreElements()){
				ze = (ZipEntry) zlist.nextElement();
				File file = new File(this.conf_file_path + "/" + ze.getName());
				OutputStream os = new BufferedOutputStream(new FileOutputStream(file));
				InputStream is = zfile.getInputStream(ze);
				int len = 0;
				while((len = is.read(buffer,0,1024)) != -1){
					os.write(buffer, 0, len);
				}
				
				is.close();
				os.close();
			}
			
			zfile.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		HadoopConfigWatcher watcher = new HadoopConfigWatcher(args[0]);
		watcher.handleConfig();
		Thread.sleep(Long.MAX_VALUE);
	}
	
}
