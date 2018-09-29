package com.alg.mr.mapjoin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Map<Integer, String> map = new HashMap<Integer, String>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		BufferedReader br = null;
		String line = null;
		if (context.getCacheFiles() != null
				&& context.getCacheFiles().length > 0) {
			br = new BufferedReader(new FileReader(new File(
					"./dept.csv")));
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split(",");
				map.put(Integer.parseInt(tokens[0]), tokens[1]);
			}
		}
		br.close();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		int deptno = Integer.parseInt(fields[3]);
		String dname = map.get(deptno);
		context.write(new Text(fields[1]), new Text(dname));
	}

}
