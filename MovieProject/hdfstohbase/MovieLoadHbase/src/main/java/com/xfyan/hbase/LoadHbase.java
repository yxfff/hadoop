package com.xfyan.hbase;

import java.io.IOException;
import java.util.Iterator;

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

public class LoadHbase {
	public static class MyMapper extends Mapper<Object,Text,Text,Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//"用户编号::性别::年龄::电影编号::电影评分::时间戳::电影名"
			String[] str = value.toString().split("::");
			String user_id = str[0];
			int index = value.toString().indexOf("::");
			String movie_info = value.toString().substring(index+2);
			context.write(new Text(user_id), new Text(movie_info));
			
		}
	}
	
	public static class MyReducer extends TableReducer<Text,Text,ImmutableBytesWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			//"用户编号  ,     性别::年龄::电影编号::电影评分::时间戳::电影名"
			// rowkey: user_id
			// column family: movie_id
			// column: movie_name value:xx
			// column: gender value:female/male
			// column: age value:xx
			// column: score value:xx
			// timestamp n - time
			byte[] user_id = key.getBytes();
			ImmutableBytesWritable rkey = new ImmutableBytesWritable(user_id);
			Put put = new Put(user_id);
			Iterator it = values.iterator();
			long n = 2872005692000L;
			
			while(it.hasNext()){
				String[] res = it.next().toString().split("::");
				byte[] movie_id = res[2].getBytes();
				byte[] gender = res[0].getBytes();
				byte[] age = res[1].getBytes();
				byte[] movie_name = res[5].getBytes();
				byte[] movie_score = res[3].getBytes();
				long timestamp =(n - Integer.parseInt(res[4].toString()));
				put.add(movie_id,Bytes.toBytes("movie_name"),timestamp,movie_name);
				put.add(movie_id,Bytes.toBytes("gender"),timestamp,gender);
				put.add(movie_id,Bytes.toBytes("age"),timestamp,age);
				put.add(movie_id,Bytes.toBytes("score"),timestamp,movie_score);
			}
			
			context.write(new ImmutableBytesWritable(user_id), put);
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf,"movie table");
		job.setJarByClass(LoadHbase.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		
		TableMapReduceUtil.initTableReducerJob("movie_table", MyReducer.class, job);
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
