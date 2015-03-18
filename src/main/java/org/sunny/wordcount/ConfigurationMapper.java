package org.sunny.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ConfigurationMapper {
	Configuration conf = new Configuration();
	public void addConfFile(String fileName) {
		conf.addResource(new Path(fileName));
	}
	public void addConfFileFromClasspath(String fileName) {
		conf.addResource(fileName);
	}
	
	public void setProperty(String name, String value) {
		conf.set(name, value);
	}
	
	public String getProperty(String name) {
		return conf.get(name);
	}
}
