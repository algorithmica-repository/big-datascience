package com.alg.mr.aggregate;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

	private int max;
	
	@Override
	protected void cleanup(
			Context context)
			throws IOException, InterruptedException {
		context.write(new IntWritable(1), new IntWritable(max));
	}

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String[] fields  = value.toString().split(",");
		int sal = Integer.parseInt(fields[2]);
		if(sal > max)  max = sal;
	}

	@Override
	protected void setup(
			Context context)
			throws IOException, InterruptedException {
		max = 0;
	}

}
