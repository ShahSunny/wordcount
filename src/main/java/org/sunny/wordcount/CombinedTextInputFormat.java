package org.sunny.wordcount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


public class CombinedTextInputFormat extends CombineFileInputFormat<Text, NullWritable> {
	
	public CombinedTextInputFormat() {
		super();
		setMaxSplitSize(1024 * 1024 * 64);
	}
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}
	
	@Override
	public RecordReader<Text, NullWritable> createRecordReader(InputSplit inputSplit,
			TaskAttemptContext taskContext) throws IOException {
		CombineFileSplit combineFileSplit = (CombineFileSplit) inputSplit; 
		return new CombinedTextInputFormatReader(combineFileSplit, taskContext);
	}
}
