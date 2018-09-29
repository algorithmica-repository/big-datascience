package com.alg.mapred.aggregation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregatorMapper extends Mapper<Object, Text, IntWritable, IntWritable> {	
	private int max;
	private IntWritable one = new IntWritable(1);

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		context.write(one, new IntWritable(max));
	}

	@Override
	protected void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		int current = Integer.parseInt(value.toString());
		if(current > max)
			max = current;
	}

	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		max = -1;
	}
	

}
