package com.xfyan.hbase;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ImportHbase {
	
	//hadoop jar ImprotHbase.jar /Seplog/* 
	public static class myMapper extends Mapper<Object,Text,Text,Text>{

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
//			super.map(key, value, context);
			
			String line = value.toString();
			String str[] = line.split(" ");
			String ip = str[0];
			String time = str[3].substring(1);
			String dir = str[6];
			
			long ipnum = Ip2Int(ip);
			long timenum = TimeToLong(time);
			String outkey = ipnum+"-"+ timenum;
			context.write(new Text(outkey), new Text(dir));
		}

		private long TimeToLong(String time) {
			SimpleDateFormat date_format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
			Date date;
			long newdate = 0;
			try {
				date = date_format.parse(time);
				newdate = date.getTime()/1000;
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			return newdate;
		}

		private long Ip2Int(String ip) {
			
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
			 
//			 String strs[]=ip_string.split("\\.");
//		     Integer[] ip_addr=new Integer[4];
//		     for(int i=0;i<strs.length;i++){
//		         	System.out.println(strs[i]);
//		        	ip_addr[i]=Integer.parseInt(strs[i]);		
//		     }
//		     int ip=(ip_addr[0]<<24)+(ip_addr[1]<<16)+(ip_addr[2]<<8)+ip_addr[3];
//             return ip;
			
		}
		
	}
	
	public static class Import2HbaseReducer extends TableReducer<Text,Text,ImmutableBytesWritable>{

		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
//			super.reduce(arg0, arg1, arg2);
			byte[] bytes = key.getBytes();
			ImmutableBytesWritable rkey = new ImmutableBytesWritable(bytes);
			Put put = new Put(bytes);
			byte[] dir = value.iterator().next().getBytes();
			put.add(Bytes.toBytes("info"),Bytes.toBytes("url"),dir);
			
			context.write(rkey, put);
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf,"hbase");
		job.setJarByClass(ImportHbase.class);
		
		job.setMapperClass(myMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob("access-log", Import2HbaseReducer.class,job);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
