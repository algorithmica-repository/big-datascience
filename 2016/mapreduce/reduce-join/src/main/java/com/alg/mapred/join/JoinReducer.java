package com.alg.mapred.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		List<String> employees = new ArrayList<String>();
		List<String> depts = new ArrayList<String>();

		for (Text val : values) {
			String currValue = val.toString();
			String splitVals[] = currValue.split("#");  
			if (splitVals[0].equals("D")) {
				depts.add(splitVals[1]);
			} else {
				employees.add(splitVals[1]);
			}
		}
		
		for(String dept:depts) {
			for(String emp:employees) {
				context.write(key, new Text(dept + " " + emp));
			}
		}
		
		
	}

}
