package com.xfyan.orderby;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OrderBy {
	public static class TwoFieldKey implements WritableComparable<TwoFieldKey>{
		
		private Text company;
		private IntWritable orderNumber;
		
		public TwoFieldKey() {
			company = new Text();
			orderNumber = new IntWritable();
		}
		
		public TwoFieldKey(Text company,IntWritable orderNumber){
			this.company = company;
			this.orderNumber = orderNumber;
		}
		
		public Text getCompany(){
			return this.company;
		}
		
		public IntWritable getOrderNumber(){
			return this.orderNumber;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			company.readFields(in);
			orderNumber.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			company.write(out);
			orderNumber.write(out);
		}

		@Override
		public int compareTo(TwoFieldKey other) {
			if(this.company.compareTo(other.getCompany())==0){
				return this.orderNumber.compareTo(other.getOrderNumber());
			}else{
				return -this.company.compareTo(other.getCompany());
			}
		}
		
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof TwoFieldKey){
				TwoFieldKey r = (TwoFieldKey) obj;
				return r.getCompany().equals(company);
			}else{
				return false;
			}
		}
		
	}
	
	public static class mapper extends Mapper<Object,Text,TwoFieldKey,NullWritable>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, TwoFieldKey, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String inputString = value.toString();
			String[] splits = inputString.split(" ");
			TwoFieldKey outputkey = new TwoFieldKey(new Text(splits[0]),new IntWritable(Integer.parseInt(splits[1])));
			context.write(outputkey, NullWritable.get());
		}
	}
	
	public static class PartByCompanyPartitioner extends Partitioner<TwoFieldKey,NullWritable>{

		@Override
		public int getPartition(TwoFieldKey key, NullWritable value, int numPartitions) {
			String company = key.getCompany().toString();
			int firstchar = (int)company.charAt(0);
			return (numPartitions -firstchar * numPartitions /128 -1);
		}
		
	}
	
	public static class reducer extends Reducer<TwoFieldKey,NullWritable,Text,IntWritable>{
		@Override
		protected void reduce(TwoFieldKey key, Iterable<NullWritable> values,
				Reducer<TwoFieldKey, NullWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			for(NullWritable val:values){
				Text company = key.getCompany();
				IntWritable orderNum = key.getOrderNumber();
				context.write(company, orderNum);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if(otherArgs.length != 2){
			System.err.println("Usage homeowrk");
			System.exit(2);
		}
		
		Job job = new Job(conf,"homework");
		job.setInputFormatClass(TextInputFormat.class);
		job.setJarByClass(OrderBy.class);
		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);
		job.setMapOutputKeyClass(TwoFieldKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(PartByCompanyPartitioner.class);
		job.setNumReduceTasks(5);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
