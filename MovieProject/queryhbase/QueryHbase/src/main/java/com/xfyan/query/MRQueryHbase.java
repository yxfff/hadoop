package com.xfyan.query;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRQueryHbase {
	/**
	 *row key:user_id
	 *column family:movie_id
	 *column:gender value:female/male
	 *column:age value:xx
	 *column:movie_name value:name
	 *column:score value:xx
	 */
	public class  MyMapper extends TableMapper<ImmutableBytesWritable,ImmutableBytesWritable>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable>.Context context)
				throws IOException, InterruptedException {
			String gender = context.getConfiguration().get("gender");
			String age = context.getConfiguration().get("age");
			
			int i = 0;
			String score = null;
			for(KeyValue kv:value.list()){
				if("gender".equals(Bytes.toString(kv.getQualifier())) && gender.equals(Bytes.toString(kv.getValue())) ){
					i++;
				}
				if("age".equals(Bytes.toString(kv.getQualifier())) && age.equals(Bytes.toString(kv.getValue()))){
					i++;
				}
				if("score".equals(Bytes.toString(kv.getQualifier()))){
					score = Bytes.toString(kv.getValue());
				}
				
				System.out.println(score);
				ImmutableBytesWritable mapkey = new ImmutableBytesWritable(Bytes.toBytes("score"));
				ImmutableBytesWritable mapvalue = new ImmutableBytesWritable(Bytes.toBytes(score));
				context.write(mapkey, mapvalue);
			}
		}
	}
	
	public class MyReducer extends Reducer<ImmutableBytesWritable,ImmutableBytesWritable,Text,Text>{
		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values,
				Reducer<ImmutableBytesWritable, ImmutableBytesWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int max = 0;
			int min = 0;
			int score = 0;
			int n = 0;
			
			for(ImmutableBytesWritable value : values){
				String s = Bytes.toString(value.get());
				score = Integer.parseInt(s);
				
				if(score < min){
					min = score;
				}else if(score > max){
					max = score;
				}
				
				sum+=score;
				n++;
			}
			
			int ave = sum/n;
			context.write(new Text("score result:"),new Text("ave="+ ave+ ",max="+max+",min="+min));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=3){
			System.err.println("Usage:<gender><age><output>");
			System.err.println("<gender: female/male");
			System.exit(0);
		}
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase,.zookeeper.quorum","binder2,binder3,binder4");
		conf.set("gender",args[0]);
		conf.set("age",args[1]);
		
		Job job = Job.getInstance();
		job.setJarByClass(MRQueryHbase.class);
		
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("movie_table", scan, MyMapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job,new Path(args[2]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	
	
	
	
}
