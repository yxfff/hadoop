package com.xfyan.query;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * hbase table 
 *row key:user_id
 *column family:movie_id
 *column:movie_name
 *value:score
 *给定用户和电影，查找score
 */
public class QueryTest {
	@SuppressWarnings({ "resource", "deprecation" })
	public static String getScore(String user_id,String movie_id){
		Configuration conf = HBaseConfiguration.create();
		conf.set(HConstants.ZOOKEEPER_QUORUM,"binder2,binder3,binder4");
		ResultScanner scanner = null;
		HTable table = null;
		
		try {
			table = new HTable(conf,"movie_table");
			Get get = new Get(Bytes.toBytes(user_id));
			get.addFamily(Bytes.toBytes(movie_id));
			Result result = table.get(get);
			
			
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(movie_id));
			scanner = table.getScanner(scan);
			Result resultInScan = null;
			while((resultInScan = scanner.next()) != null){
				for(Map.Entry<byte[], byte[]> entry : result.getFamilyMap(movie_id.getBytes()).entrySet()){
					String column = new String(entry.getKey());
					String value = new String(entry.getValue());
					//System.out.println("user:" + user_id + "movie id: "+ movie_id +" movie:" + column );
					//System.out.println("socre is :" + value);
					return value;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "can not find the score";
	}
	
	public static void main(String[] args) {
		QueryTest qh = new QueryTest();
		if(args.length != 2){
			System.out.println("<usage>: user_id movie_id");
			System.exit(0);
		}
		String value = qh.getScore(args[0], args[1]);
		System.out.println("score is " + value);
	}
	
}
