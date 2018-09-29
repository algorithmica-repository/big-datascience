package com.alg.mapred.sorting;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable>{

	//@Override
	protected void reduce(IntWritable key, Iterable<NullWritable> values,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
	
	

}
