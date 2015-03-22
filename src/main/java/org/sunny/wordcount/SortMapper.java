package org.sunny.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text, Text, IntWritable, Text> {
	@Override
	protected void map(Text key, Text value,
			Mapper<Text, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		int count = Integer.parseInt(value.toString());
		context.write(new IntWritable(count), key);
	}
}
