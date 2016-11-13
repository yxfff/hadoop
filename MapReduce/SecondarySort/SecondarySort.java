package com.xfyan.three;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 
 *二次排序：
 *原始数据：
 *4 3
 *4 2 
 *4 1
 *2 3
 *2 7
 *
 *排序后：
 *2 3
 *2 7
 *4 1
 *4 2
 *4 3
 */
public class SecondarySort {
	public class SecondaryMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(value,NullWritable.get());
		}
	}
	
	public class KeyPartitioner extends HashPartitioner<Text,NullWritable>{
		@Override
		public int getPartition(Text key, NullWritable value, int numReduceTasks) {
			return (key.toString().split(" ")[0].hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}
	
	
	public class SortCompatarator extends WritableComparator{
		protected SortCompatarator(){
			super(Text.class,true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if(Integer.parseInt(a.toString().split(" ")[0]) == Integer.parseInt(b.toString().split(" ")[0])){
				if(Integer.parseInt(a.toString().split(" ")[1]) > Integer.parseInt(b.toString().split(" ")[1])){
					return 1;
				}else if(Integer.parseInt(a.toString().split(" ")[1]) == Integer.parseInt(b.toString().split(" ")[1])){
					return 0;
				}else if(Integer.parseInt(a.toString().split(" ")[1]) < Integer.parseInt(b.toString().split(" ")[1])){
					return -1;
				}
			}else{
				if(Integer.parseInt(a.toString().split(" ")[0]) > Integer.parseInt(b.toString().split(" ")[0])){
					return 1;
				}else if(Integer.parseInt(a.toString().split(" ")[0]) < Integer.parseInt(b.toString().split(" ")[0])){
					return -1;
				}
			}
			
			return 0;
		}
	}
	
	
	
	public class SecondaryReducer extends Reducer<Text,NullWritable,NullWritable,Text>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			context.write(NullWritable.get(), key);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf,"secondary sort");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(SecondaryMapper.class);
		job.setReducerClass(SecondaryReducer.class);
		job.setPartitionerClass(KeyPartitioner.class);
		job.setSortComparatorClass(SortCompatarator.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
