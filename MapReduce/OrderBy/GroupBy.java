package cn.chinahadoop.groupby;

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

public class GroupBy {
	public static class TwoFieldKey implements WritableComparable<TwoFieldKey> {

		private Text company;
		private IntWritable orderNumber;

		public TwoFieldKey() {
			company = new Text();
			orderNumber = new IntWritable();
		}

		public TwoFieldKey(Text company, IntWritable orderNumber) {
			this.company = company;
			this.orderNumber = orderNumber;
		}

		public Text getCompany() {
			return company;
		}

		public IntWritable getOrderNumber() {
			return orderNumber;
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
			if (this.company.compareTo(other.getCompany()) == 0) {
				return this.orderNumber.compareTo(other.getOrderNumber());
			} else {
				return -this.company.compareTo(other.getCompany());
			}
		}

		@Override
		public boolean equals(Object right) {
			if (right instanceof TwoFieldKey) {
				TwoFieldKey r = (TwoFieldKey) right;
				return r.getCompany().equals(company);
			} else {
				return false;
			}
		}

		@Override
		public String toString() {
			return "Company: " + company + " orderNumber: " + orderNumber;
		}
	}

	// partitioned by Company, we can also use  TotalOrderPartitioner
	public static class PartByCompanyPartitioner extends
			Partitioner<TwoFieldKey, NullWritable> {
		@Override
		public int getPartition(TwoFieldKey key, NullWritable value,
				int numPartitions) {
			String company = key.getCompany().toString();
			int firstChar = (int) company.charAt(0);
			return firstChar * numPartitions / 128;
		}
	}

	public static class mapper extends
			Mapper<Object, Text, TwoFieldKey, NullWritable> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String intputString = value.toString();
			String[] splits = intputString.split(" ");
			TwoFieldKey outKey = new TwoFieldKey(new Text(splits[0]),
					new IntWritable(Integer.parseInt(splits[1])));
			context.write(outKey, NullWritable.get());

		}
	}

	public static class reducer extends
			Reducer<TwoFieldKey, NullWritable, Text, IntWritable> {
		public void reduce(TwoFieldKey key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			for (NullWritable val : values) {
				Text company = key.getCompany();
				IntWritable orderNumber = key.getOrderNumber();
				context.write(company, orderNumber);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: homework  ");
			System.exit(2);
		}
		Job job = new Job(conf, "homework");
		job.setInputFormatClass(TextInputFormat.class);
		job.setJarByClass(GroupBy.class);
		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);
		job.setMapOutputKeyClass(TwoFieldKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(PartByCompanyPartitioner.class);
		// set to 2 to verify where the partition take the effect.
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
