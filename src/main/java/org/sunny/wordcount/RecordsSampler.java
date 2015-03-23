package org.sunny.wordcount;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.Sampler;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Samples the first n records from s splits.
 * Inexpensive way to sample random data.
 */
public class RecordsSampler implements Sampler<Text,NullWritable> {

  protected final int numSamples;
  protected final int maxSplitsSampled;

  /**
   * Create a SplitSampler sampling <em>all</em> splits.
   * Takes the first numSamples / numSplits records from each split.
   * @param numSamples Total number of samples to obtain from all selected
   *                   splits.
   */
  public RecordsSampler(int numSamples) {
    this(numSamples, Integer.MAX_VALUE);
  }

  /**
   * Create a new SplitSampler.
   * @param numSamples Total number of samples to obtain from all selected
   *                   splits.
   * @param maxSplitsSampled The maximum number of splits to examine.
   */
  public RecordsSampler(int numSamples, int maxSplitsSampled) {
    this.numSamples = numSamples;
    this.maxSplitsSampled = maxSplitsSampled;
  }

  /**
   * From each split sampled, take the first numSamples / numSplits records.
   */
  public Text[] getSample(InputFormat<Text,NullWritable> inf, Job job) 
      throws IOException, InterruptedException {
    List<InputSplit> splits = inf.getSplits(job);
    Text[] samples = new Text[numSamples];
    int splitsToSample = Math.min(maxSplitsSampled, splits.size());
    //int samplesPerSplit = numSamples / splitsToSample;
    int records = 0;
    for (int i = 0; i < splitsToSample && records < numSamples; ++i) {
      TaskAttemptContext samplingContext = new TaskAttemptContextImpl( job.getConfiguration(), new TaskAttemptID() );
      RecordReader<Text,NullWritable> reader = inf.createRecordReader(
          splits.get(i), samplingContext);
      reader.initialize(splits.get(i), samplingContext);
      while (reader.nextKeyValue() && records < numSamples) {
    	Text line = (Text) reader.getCurrentKey();
    	String[] words = WordCountMapper.splitLines(line.toString());
    	for(String word:words) {
    		if(records < numSamples) {
    			samples[records] = new Text(word);
    			++records;
    		}
    	}              
      }
      reader.close();
    }
    return samples;
  }
}

