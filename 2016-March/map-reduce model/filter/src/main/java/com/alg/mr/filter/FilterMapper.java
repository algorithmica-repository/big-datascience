package com.alg.mr.filter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String[] fields  = value.toString().split(",");
		int sal = Integer.parseInt(fields[2]);
		if(sal > 10000) {
			context.write(new Text(fields[1]), new IntWritable(sal));
		}
	}

}
