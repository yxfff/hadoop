/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package com;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sort {

  public static class SimpleMapper 
       extends Mapper<IntWritable, Text, RevertKey, Text>{
    
    public void map(IntWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      RevertKey newKey = new RevertKey(key);
      context.write(newKey, value);
    }
  }
  
  public static class SimpleReducer 
       extends Reducer<RevertKey,Text,Text,IntWritable> {
    public void reduce(RevertKey key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
        context.write(val, key.getKey());
      }
    }
  }

  public static class RevertKey
        implements WritableComparable<RevertKey> {
    
    private IntWritable key;
    public RevertKey(){
      key = new IntWritable();
    }

    public RevertKey(IntWritable key) {
      this.key = key;
    }
    
    public IntWritable getKey() {
      return key;
    } 
    
    @Override
    public void readFields(DataInput in) throws IOException {
      key.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      key.write(out);
    }

    @Override
    public int compareTo(RevertKey other) {
      return -key.compareTo(other.getKey());
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Sort");
    job.setJarByClass(Sort.class);
    job.setMapperClass(SimpleMapper.class);
    job.setReducerClass(SimpleReducer.class);
    job.setMapOutputKeyClass(RevertKey.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
