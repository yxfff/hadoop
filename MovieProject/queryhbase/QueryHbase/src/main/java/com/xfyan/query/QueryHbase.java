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
*row key:user_id
*column family:movie_id
*column:gender value:female/male
*column:age value:xx
*column:movie_name value:name
*column:score value:xx
timestamp: n - time
*/

public class QueryHbase {
	@SuppressWarnings({ "resource", "deprecation" })
	public static String getScore(String user_id,String movie_id){
		Configuration conf = HBaseConfiguration.create();
		conf.set(HConstants.ZOOKEEPER_QUORUM,"binder2,binder3,binder4");
		ResultScanner scanner = null;
		HTable table = null;
		
		try {
			table = new HTable(conf,"movie_table");
			Get get = new Get(Bytes.toBytes(user_id));
			get.addColumn(Bytes.toBytes(movie_id), Bytes.toBytes("score"));
			Result result = table.get(get);
			
			String key = Bytes.toString(result.getRow());
			String value = Bytes.toString(result.getValue(Bytes.toBytes(movie_id), Bytes.toBytes("score")));
			
			System.out.println("user_id:" + user_id + " to movie_id:" + movie_id +" score is");
			System.out.println(value);
			
			return value;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "can not find the score";
	}
	
	public static void main(String[] args) {
		QueryHbase qh = new QueryHbase();
		if(args.length != 2){
			System.out.println("<usage>: user_id movie_id");
			System.exit(0);
		}
		String value = qh.getScore(args[0], args[1]);
		System.out.println("score is " + value);
	}
	
}
