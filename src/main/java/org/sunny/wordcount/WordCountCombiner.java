package org.sunny.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class WordCountCombiner extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	Logger logger = Logger.getLogger(WordCountCombiner.class);
	int threasoldForCount = 10;
	
	protected boolean shallWrite(int count) {
		return true;
	}
	
	protected int sumupValues(Iterable<IntWritable> values) {
		int count = 0;
		for(IntWritable value:values) {
			count += value.get();
		}
		return count;
	}
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int count = sumupValues(values);
		if(shallWrite(count)) {
			context.write(key,new IntWritable(count));
		}
	}
}
