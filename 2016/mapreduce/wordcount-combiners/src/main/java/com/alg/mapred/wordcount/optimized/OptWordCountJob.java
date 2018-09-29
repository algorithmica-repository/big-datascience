package com.alg.mapred.wordcount.optimized;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OptWordCountJob {

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("OptWordCount");
		
		job.setJarByClass(OptWordCountJob.class);
		job.setMapperClass(OptWordCountMapper.class);
		job.setCombinerClass(OptWordCountReducer.class);
		job.setReducerClass(OptWordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
