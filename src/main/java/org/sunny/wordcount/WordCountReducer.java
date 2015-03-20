package org.sunny.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	Logger logger = Logger.getLogger(WordCountReducer.class);
	int threasoldForCount = 10;
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws IOException ,InterruptedException {
		super.setup(context);
		Configuration conf =  context.getConfiguration();
		threasoldForCount = conf.getInt("threasold-for-count", 10);
		logger.info("*****************************************************************");
		logger.info("In setup");
		logger.info("*****************************************************************");
		
		FileSystem.create(FileSystem.get(conf), new Path("/tmp/threasoldForCount_" + threasoldForCount), FsPermission.getDefault());
		logger.info("threasold-for-count = " + threasoldForCount);
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
