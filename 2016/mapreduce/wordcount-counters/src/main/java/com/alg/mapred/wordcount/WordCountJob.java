package com.alg.mapred.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alg.mapred.wordcount.WordCountMapper.CUSTOM_MAP_COUNTERS;
import com.alg.mapred.wordcount.WordCountReducer.CUSTOM_REDUCE_COUNTERS;

public class WordCountJob {

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("WordCount-Counters");
		job.setJarByClass(WordCountJob.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		Counters counters = job.getCounters();
		Counter c1 = counters.findCounter(CUSTOM_MAP_COUNTERS.LINECOUNT);
		System.out.println(c1.getDisplayName()+":"+c1.getValue());
		Counter c2 = counters.findCounter(CUSTOM_REDUCE_COUNTERS.DISTINCT_WORDS);
		System.out.println(c2.getDisplayName()+":"+c2.getValue());
	}

}
