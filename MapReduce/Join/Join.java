package com.xfyan.MR.two;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *student_info:
 *Amy 00001
 *Tom 00002
 *Binder 00003
 *
 *student_class_info:
 *00001 Chinese
 *00002 Math
 *00003 English
 *
 * print:
 * Amy Chinese
 * Tom Math
 * Binder English
 *
 */
public class Join {
	public static final String LEFT_FILENAME = "student_info.txt";
	public static final String RIGHT_FILENAME = "student_class_info.txt";
	public static final String LEFT_FLAG = "l";
	public static final String RIGHT_FLAG = "r";
	
	public class JoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
			String fileFlag = null;
			String joinKey = null;
			String joinValue = null;
			
			if(filePath.contains(LEFT_FILENAME)){
				fileFlag = LEFT_FLAG;
				joinKey = value.toString().split("\t")[1];
				joinValue = value.toString().split("\t")[0];
			}else{
				fileFlag = RIGHT_FLAG;
				joinKey = value.toString().split("\t")[0];
				joinValue = value.toString().split("\t")[1];
			}
			
			context.write(new Text(joinKey),new Text(joinValue + "\t" + fileFlag));
		}
	}
	
	
	public class JoinReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it = values.iterator();
			
			List<String> studentClassNames = new ArrayList<String>();
			String studentName ="";
			while(it.hasNext()){
				String[] infos = it.next().toString().split("\t");
				if(infos[1].equals(LEFT_FLAG)){
					studentName = infos[0];
				}else{
					studentClassNames.add(infos[0]);
				}
			}
			
			for(int i = 0; i < studentClassNames.size();i++){
				context.write(new Text(studentName),new Text(studentClassNames.get(i)));
			}
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf,"Join");
		job.setJarByClass(Join.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
