package org.sunny.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	public static String[] splitLines(String line) {
		return line.split("((?!_)\\p{Punct}|\\s)+",0);
	}
	@Override
	public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	    
	    String line = value.toString();
	    String[] words = splitLines(line);
	    for(String word:words) {
	    	if(word.length() > 1) {
	    		context.write(new Text(word), new IntWritable(1));
	    	}
	    }
	  }
}
