package com.alg.mr.grep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GrepMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	private List<String> words = new ArrayList<String>();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		for(String word:words) {
			if(line.contains(word)) {
				context.write(value, NullWritable.get());
				break;
			}
		}
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		BufferedReader br = null;
		String line = null;
		if (context.getCacheFiles() != null
				&& context.getCacheFiles().length > 0) {
			br = new BufferedReader(new FileReader(new File(
					"./words.txt")));
			while ((line = br.readLine()) != null) {
				words.add(line);
			}
		}
		br.close();
	}

}
