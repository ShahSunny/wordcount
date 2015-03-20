package org.sunny.wordcount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.log4j.Logger;

public class CombinedTextInputFormatReader extends
		RecordReader<LongWritable, Text> {

	Logger logger = Logger.getLogger(CombinedTextInputFormatReader.class);
	private Path[] paths;
	private LongWritable key;
	private Text value;

	public CombinedTextInputFormatReader(CombineFileSplit inputSplit, TaskAttemptContext taskContext) {
		paths = inputSplit.getPaths();
		logger.info("******************************************************Start********************");
		for(Path path:paths) {
			logger.info(path.toString());
		}
		logger.info("******************************************************End********************");
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return false;
	}

}
