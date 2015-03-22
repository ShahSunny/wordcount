package org.sunny.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
	@Override
	protected void map(Text key, IntWritable value,
			Mapper<Text, IntWritable, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		context.write(value, key);
	}
}
