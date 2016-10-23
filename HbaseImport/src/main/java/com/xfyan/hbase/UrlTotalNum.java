package com.xfyan.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class UrlTotalNum {
	public static class myMapper extends TableMapper<ImmutableBytesWritable,ImmutableBytesWritable>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable>.Context context)
				throws IOException, InterruptedException {
			String rowkey,ip;
			for(KeyValue kv : value.list()){
				rowkey = Bytes.toString(kv.getRow());
//				System.out.println(rowkey);
				String[] str = rowkey.split("-");
				String ip2 = str[1];
				ip = IntToIp(Long.parseLong(ip2));
				ImmutableBytesWritable outkey = new ImmutableBytesWritable(Bytes.toBytes(ip));
				ImmutableBytesWritable outvalue = new ImmutableBytesWritable(Bytes.toBytes(1));
				context.write(outkey, outvalue);
						
			}
		}

		private String IntToIp(long ip) {
			StringBuffer sb = new StringBuffer("");
			sb.append(String.valueOf(ip >>>24) + ".");
			sb.append(String.valueOf((ip & 0x00FFFFFF) >>> 16) + ".");
			sb.append(String.valueOf((ip & 0x0000FFFF) >>>8)+ ".");
			sb.append(String.valueOf(ip & 0x000000FF));
			return sb.toString();
		}
	}
	
	public static class myReducer extends TableReducer<ImmutableBytesWritable,ImmutableBytesWritable,ImmutableBytesWritable>{
		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values,
				Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			int sum =0;
			for(ImmutableBytesWritable value: values){
				sum++;
			}
			
			Put put = new Put(key.get());
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("totalNum"), Bytes.toBytes(new String(sum + "")));
			context.write(key, put);
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "binder2,binder3,binder4");
		Job job = Job.getInstance(conf,"hbase");
		job.setJarByClass(UrlTotalNum.class);
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("access-log", scan, myMapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("totalip", myReducer.class, job);
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
