package org.sunny.wordcount;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.AccessControlException;

public class RecordReaderInternal {
	
	private Path[] paths;
	private int currentFilePos = -1;
	Configuration conf;
	CompressionCodecFactory compressionCodecFactory;
	BufferedReader br = null;
	String line = null;
	public RecordReaderInternal(Path[] paths, Configuration conf) {
		this.paths = paths;
		this.conf = conf;
		compressionCodecFactory = new CompressionCodecFactory(conf);
		createNewReader();
	}

	private void createNewReader() {
		closeBufferedReader();
		++currentFilePos;
		if( currentFilePos < paths.length) {
			Path currentPath = paths[currentFilePos];
			CompressionCodec codec = compressionCodecFactory.getCodec(currentPath);			
			InputStream filein;
			try {
				filein = FileContext.getFileContext(currentPath.toUri(),conf).open(currentPath);
				if(codec != null){
					filein = codec.createInputStream(filein);
				}
				br = new BufferedReader(new InputStreamReader(filein));
			} catch (AccessControlException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedFileSystemException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void closeBufferedReader() {
		if(br != null) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		br = null;
		line = null;
	}

	public void close() {
		closeBufferedReader();
		currentFilePos = -1;
	}

	public long getPosition() {
		if(currentFilePos == -1)
			return 0;
		else 
			return currentFilePos;
	}
	
	public float getProgress() {
		float progress = 1;
		if(paths.length > 0) {			
			progress = (float) ((getPosition() * 1.0) / paths.length);
		}
		return progress;
	}
	
	public boolean nextKeyValue() {		
		line = null;
		while(br != null && line == null) {
			try {
				line = br.readLine();				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			if(line == null) {
				createNewReader();
			}
		}		
		
		boolean isReadSuccessful = (line != null); 
		return isReadSuccessful;
	}
	

	public String getCurrentLine() {
		return line;
	}

}
