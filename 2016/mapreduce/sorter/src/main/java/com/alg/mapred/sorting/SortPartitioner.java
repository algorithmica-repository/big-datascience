package com.alg.mapred.sorting;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public class SortPartitioner extends org.apache.hadoop.mapreduce.Partitioner<IntWritable, NullWritable> {	
	private int minkey = 1;	
	private int maxkey = 600;

	@Override
	public int getPartition(IntWritable key, NullWritable value,
			int numPartitions) {
		int range = (maxkey - minkey + 1)/numPartitions;
		return (key.get() - minkey) / range;
	}
	

}
