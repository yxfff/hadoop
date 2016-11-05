package com.xfyan.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Join {
	public static class MyMapper extends Mapper<Object,Text,Text,Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			//users.dat + ratings.dat
			//users.dat--->用户编号::性别::年龄::职业
			//ratings.dat--->用户编号::电影编号::电影评分::时间戳
			//user表:map ->("用户编号","users+性别::年龄")
			//ratings.dat: map ->("用户编号","ratings+电影编号::电影评分::时间戳")
			
			String line = value.toString();
			String relationType = new String();
			String filename =((FileSplit)context.getInputSplit()).getPath().toString();
			
			String[] values = line.split("::");
			
			Text outputKey = null;
			Text outputValue = null;
			
			if(filename.contains("user.dat")){
				relationType = "users";
				outputKey = new Text(values[0]);
				outputValue = new Text(relationType + "+" + values[1]+"::" + values[2]);
			}else if(filename.contains("ratings.dat")){
				relationType = "ratings";
				outputKey=new Text(values[0]);
				outputValue = new Text(relationType + "+" + values[1] + "::" +values[2] + "::" +values[3]);
			}
			
			context.write(outputKey,outputValue);
		}
	}
	
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//user表:map ->("用户编号","users+性别::年龄")
			//ratings.dat: map ->("用户编号","ratings+电影编号::电影评分::时间戳")
			//context.write("用户编号","用户编号::性别::年龄::电影编号::电影评分::时间戳");
			//output--->user_rating.dat
			List<String> users = new ArrayList<String>();
			List<String> ratings = new ArrayList<String>();
			Iterator it = values.iterator();
			while(it.hasNext()){
				String record = it.next().toString();
				int index = record.indexOf("+");
				String relationType = record.substring(0, index);
				
				if("users".equals(relationType)){
					users.add(record.substring(index+1));
				}else if("ratings".equals(relationType)){
					ratings.add(record.substring(index+1));
				}
				
				if(users.size() > 0 && ratings.size() > 0){
					for(String user : users){
						for(String rating : ratings){
							context.write(new Text(key), new Text(key + "::" + user + "::" + rating));
						}
					}
				}
			}
		}
	}
	
	
	public static class MyMapper2 extends Mapper<Object,Text,Text,Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			//movie.dat + user_rating.dat
			//movie.dat-->"电影编号::电影名::类别"
			//user_rating.dat --> "用户编号   用户编号::性别::年龄::电影编号::电影评分::时间戳"
			//------------------------------------------
			//movie.dat map--->("电影编号","movies+电影名")
			//user_rating.dat map-->("电影编号","user_ratings+用户编号::性别::年龄::电影编号::电影评分::时间戳")
			
			
			String fileName = ((FileSplit)context.getInputSplit()).getPath().toString();
			String str = null;
			if(!fileName.contains("movie.dat")){
				StringTokenizer line = new StringTokenizer(value.toString());
				int i = 0;
				String[] strs = new String[2];
				
				while(line.hasMoreTokens()){
					strs[i] = line.nextToken();
					i++;
				}
				str = strs[1];
			}else{
				str = value.toString();
			}
			
			
			
			String relationType = new String();
			String[] values = str.split("::");
			
			Text outputKey = null;
			Text outputValue = null;
			if(fileName.contains("movies.dat")){
				relationType="movies";
				outputKey=new Text(values[0]);
				outputValue = new Text(relationType + "+" + values[1]);
			}else{
				relationType="users_ratings";
				outputKey = new Text(values[3]);
				outputValue = new Text(relationType + "+" + values[0] + "::" + values[1] + "::" + values[2] + "::"
						+ values[3] + "::" + values[4] + "::" + values[5]);
			}
			
			context.write(outputKey,outputValue);
		}
	}
	
	public static class MyReducer2 extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			//movie.dat map--->("电影编号","movies+电影名")
			//user_rating.dat map-->("电影编号","user_ratings+用户编号::性别::年龄::电影编号::电影评分::时间戳")
			//---------------------------------------------
			//context.write("","用户编号::性别::年龄::电影编号::电影评分::时间戳::电影名")
			List<String> users_ratings = new ArrayList<String>();
			List<String> movies = new ArrayList<String>();
			
			Iterator it = values.iterator();
			while(it.hasNext()){
				String record = it.next().toString();
				
				int index = record.indexOf("+");
				String relationType = record.substring(0,index);
				
				if("users_ratings".equals(relationType)){
					users_ratings.add(record.substring(index+1));
				}else if("movies".equals(relationType)){
					movies.add(record.substring(index+1));
				}
			}
			
			if(users_ratings.size() > 0 && movies.size() > 0){
				for(String user_rating : users_ratings){
					for(String movie: movies){
						context.write(new Text(""), new Text(user_rating + "::" + movie));
					}
				}
			}
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length != 5){
			System.err.println("Usage: data join <users.dat><ratings.dat><movies.dat><outdir>");
			System.exit(0);
		}
		
		Job job1 = Job.getInstance(conf,"data join");
		job1.setJarByClass(Join.class);
		job1.setMapperClass(MyMapper.class);
		job1.setReducerClass(MyReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1,new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job1,new Path(otherArgs[1]));
		
		FileOutputFormat.setOutputPath(job1,new Path(otherArgs[2]));
		job1.waitForCompletion(true);
		
		
		Job job2 = Job.getInstance();
		job2.setJarByClass(Join.class);
		job2.setMapperClass(MyMapper2.class);
		job2.setReducerClass(MyReducer2.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2,new Path(otherArgs[2]));
		FileInputFormat.addInputPath(job2,new Path(otherArgs[3]));
		
		FileOutputFormat.setOutputPath(job2,new Path(otherArgs[4]));
		
		System.exit(job2.waitForCompletion(true)?0:1);
		
		
		
		
		
	}
}
