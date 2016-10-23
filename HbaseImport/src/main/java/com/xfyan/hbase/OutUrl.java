package com.xfyan.hbase;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class OutUrl {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "binder2,binder3,binder4");
		HTable table = new HTable(conf, "access-log");

		Scan scan = new Scan();
		//String ip = args[0];
		String ip = "175.44.19.36";
		String pre = IpToInt(ip)+"";
		scan.setFilter(new PrefixFilter(Bytes.toBytes(ip)));
		ResultScanner rs = table.getScanner(scan);
		System.out.println(ip+"");
		for(Result r : rs){
			byte[] value = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("url"));
			System.out.println(Bytes.toString(value));
//			for(KeyValue kv : r.raw()){
//				byte[] rowlist = kv.getRow();
//				String[] row_str = rowlist.toString().split("-");
//				Date time = new Date(Integer.parseInt(row_str[1]));
//				int hour = time.getHours();
//				if(hour > 0 && hour < 12){
//					//todo
//				}
//			}
		}
		table.close();

	}

	private static long IpToInt(String ip) {

		long num = 0;
		String[] ips = ip.split("\\.");
		try {
			if (ips.length == 4){  
				num = Long.parseLong(ips[0], 10) * 256L * 256L * 256L + Long.parseLong(ips[1], 10) * 256L * 256L + Long.parseLong(ips[2], 10) * 256L + Long.parseLong(ips[3], 10);  
				num = num >>> 0;  
			}
		} catch (NumberFormatException e) {
			System.out.println(ip);
		}

		return num;
	}
}
