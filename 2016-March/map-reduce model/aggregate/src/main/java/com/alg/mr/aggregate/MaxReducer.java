package com.alg.mr.aggregate;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {

	@Override
	protected void reduce(
			IntWritable key,
			Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		int max = 0;
		for(IntWritable value:values) {
			if(value.get() > max) 
				max = value.get();
		}
		context.write(new IntWritable(max),NullWritable.get());
	}

}
