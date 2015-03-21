package org.sunny.wordcount;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.AccessControlException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class TestRecordReaderInternal {

	static Configuration conf = new Configuration();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		conf.set("fs.defaultFS", "file:///");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		
	}

	@After
	public void tearDown() throws Exception {
	}
	
	private List<String> readAll(RecordReaderInternal recordReaderInternal) {
		List<String> recordsRead = new LinkedList<String>();
		while(recordReaderInternal.nextKeyValue()) {
			String strValue = recordReaderInternal.getCurrentLine();
			if(strValue != null) {
				recordsRead.add(strValue);
			}
		}
		return recordsRead;  
	}
	private void writeData(Path fileName,Configuration conf, String... data) {
		try {
			FileContext fileContext = FileContext.getFileContext(fileName.toUri(),conf);			
			FSDataOutputStream outputStream = fileContext.create(fileName, EnumSet.of(CreateFlag.CREATE,CreateFlag.OVERWRITE));
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
			for(String d:data) {
				writer.write(d);
				writer.newLine();
			}
			writer.close();
			fileContext.deleteOnExit(fileName);
		} catch (UnsupportedFileSystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccessControlException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileAlreadyExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParentNotDirectoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	void displayArray(List<String> data, String name) {
		System.out.println(" [ " + name + " ]");
		for(String d:data) {
			System.out.println(d);
		}
	}
	
	@Test
	public void testBasicReadWithTwoFiles() {
		String data[] = {"one","two","three","four","five","six","seven"};
		String fileName1 = "file:///tmp/testRecordReader1.txt";
		String fileName2 = "file:///tmp/testRecordReader2.txt";
		String fileName3 = "file:///tmp/testRecordReader3.txt";
		String fileName4 = "file:///tmp/testRecordReader4.txt";
		
		Path[] paths = new Path[4];
		paths[0] = new Path(fileName1);
		paths[1] = new Path(fileName2);
		paths[2] = new Path(fileName3);
		paths[3] = new Path(fileName4);
		
		writeData(paths[0], conf, data[0], data[1], data[2], data[3]);
		writeData(paths[1], conf, data[4], data[5]);
		writeData(paths[2], conf);
		writeData(paths[3], conf,data[6]);
		
		RecordReaderInternal recordReader = new RecordReaderInternal(paths, conf);
		List<String> list = readAll(recordReader);
		displayArray(list, "Expected");
		Assert.assertArrayEquals(data, list.toArray());
		
		
	}

}
