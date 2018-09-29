package com.alg.mapred.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String commonKey = "E#";
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String tokens [] = value.toString().split(",");        
        context.write(new Text(tokens[3]), new Text(commonKey + tokens[1]));
	}

}
