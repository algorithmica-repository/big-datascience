package com.alg.mr.recordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RecordCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable>{

	@Override
	protected void reduce(
			IntWritable key,
			Iterable<IntWritable> values,
			Reducer<IntWritable, IntWritable, IntWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		int total = 0;
		for(IntWritable val:values) {
			total = total + val.get();
		}
		context.write(new IntWritable(total), NullWritable.get());
	}

}
