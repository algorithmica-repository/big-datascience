package com.alg.mr.unigrams;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UnigramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		int totalfreq = 0;
		for(IntWritable freq:values) {
			totalfreq += freq.get();
		}
		context.write(key, new IntWritable(totalfreq));
	}

}
