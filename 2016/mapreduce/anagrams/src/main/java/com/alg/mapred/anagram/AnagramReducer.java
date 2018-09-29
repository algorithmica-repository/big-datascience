package com.alg.mapred.anagram;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnagramReducer extends Reducer<Text, Text, Text, IntWritable> {
	private IntWritable outputValue = new IntWritable();

	public void reduce(Text anagramKey, Iterable<Text> anagramValues,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		for (Text anagram : anagramValues) {
			++count;
		}
		outputValue.set(count);
		context.write(anagramKey, outputValue);
	}

}
