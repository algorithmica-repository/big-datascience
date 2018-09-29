package com.alg.mapred.anagram;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AnagramJob {

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("AnagramJob");
		job.setJarByClass(AnagramJob.class);
		
		job.setMapperClass(AnagramMapper.class);
		job.setReducerClass(AnagramReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
			
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
