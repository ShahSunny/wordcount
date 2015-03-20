package org.sunny.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	int threasoldForCount = 10;
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws IOException ,InterruptedException {
		super.setup(context);
		Configuration conf =  context.getConfiguration();
		threasoldForCount = conf.getInt("threasold-for-count", 10);
	};
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable value:values) {
			count += value.get();
		}
		if(count > threasoldForCount)
			context.write(key,new IntWritable(count));
	}
}
