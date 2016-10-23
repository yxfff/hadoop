package com.xfyan.zoo;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ConnectWatcher implements Watcher{
	
	private static final int SESSION_TIMEOUT=5000;
	protected ZooKeeper zk;
	
	public void connect (String hosts) throws IOException {
		zk = new ZooKeeper(hosts,SESSION_TIMEOUT,this);
		System.out.println("connect...");
	}
	public void close() throws InterruptedException{
		zk.close();
	}
	public void process(WatchedEvent event) {
		System.out.println("process " + event.getType() + " event!!" );
	}

}
