package com.alg.mapred.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public static enum CUSTOM_MAP_COUNTERS {
    	LINECOUNT
    };
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
      context.getCounter(CUSTOM_MAP_COUNTERS.LINECOUNT).increment(1);
    }

}
