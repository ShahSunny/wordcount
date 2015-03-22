package org.sunny.wordcount;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;

import org.apache.commons.collections.map.IdentityMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.lib.IdentityReducer;
//import org.apache.hadoop.mapred.lib.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.sunny.wordcount.DI.InjectLogger;

import com.google.inject.Inject;

public class AppRunner extends Configured implements Tool {
	final String tmpLocation = "/tmpLocation/";
	@InjectLogger Logger logger;
	@Inject
	public AppRunner() {
		super();
		
	}
	
	
	public int run(String[] args) throws Exception {
		logger.debug("In AppRunner::Run");
		Configuration conf = getConf();
		conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
		conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);
		//conf.set("fs.defaultFS", "file:///");
		//conf.set("mapreduce.framework.name", "local");
		setConf(conf);
		Job job = Job.getInstance(getConf());
        job.setJobName("Word Count");
        
        //setting the class names
        job.setJarByClass(AppRunner.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountCombiner.class);        
        job.setNumReduceTasks(1);
        //setting the output data type classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(CombinedTextInputFormat.class);

        Path[] paths = filterOutPaths(args[0]);
        //to accept the hdfs input and outpur dir at run time        
        FileInputFormat.setInputPaths(job, paths);
        String outputLocation = args[1] + tmpLocation;
        FileOutputFormat.setOutputPath(job, new Path(outputLocation));

        boolean success = job.waitForCompletion(true);
    	if(success) {
    		sortData(outputLocation,args[1]);
    	} else {
    		System.exit(1);
    	}
    	
		return 0;
	}


	private void sortData(String outputLocation, String inputLocation) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(getConf());
        job.setJobName("Word Count Sort");
        //setting the class names
        job.setJarByClass(AppRunner.class);
        job.setMapperClass(SortMapper.class);                        
        job.setNumReduceTasks(1);
        //setting the output data type classes
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //to accept the hdfs input and outpur dir at run time        
        FileInputFormat.setInputPaths(job, new Path(inputLocation));        
        FileOutputFormat.setOutputPath(job, new Path(outputLocation));
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
	}


	private Path[] filterOutPaths(String path) {
		LinkedList<Path> listOfInputFiles = new LinkedList<Path>();		
		try {
			final FileSystem fs = FileSystem.get(new URI(path),getConf());
			RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(path), true);
			while(fileStatusListIterator.hasNext()){
		        LocatedFileStatus fileStatus = fileStatusListIterator.next();
		        if(fileStatus.isFile()) {
		        	if(fileStatus.getPath().getName().matches("^[a-zA-Z0-9].*((\\.c)|(\\.cpp)|(\\.h))$")) {		        	
		        		listOfInputFiles.add(fileStatus.getPath());
		        	}
		        }
			}
			return listOfInputFiles.toArray(new Path[listOfInputFiles.size()]);			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return null;
	}
}
