package com.alg.mr.mapjoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JoinJob {
	
	public static void main(String[] args) throws Exception  {
		Job job = Job.getInstance();
		job.setJobName("MapSideJoin");
		job.setJarByClass(JoinJob.class);
		job.setMapperClass(JoinMapper.class);
		job.setNumReduceTasks(0);
		job.addCacheFile(new Path(args[0]).toUri());
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.addInputPath(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
	}

}
