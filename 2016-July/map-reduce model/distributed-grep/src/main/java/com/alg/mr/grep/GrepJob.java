package com.alg.mr.grep;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GrepJob {
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("distributed-grep");
		job.setJarByClass(GrepJob.class);
		job.setMapperClass(GrepMapper.class);
		job.setNumReduceTasks(0);
		job.addCacheFile(new Path(args[2]).toUri());
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);		

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
	

}
