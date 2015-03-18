package org.sunny.wordcount;

import org.apache.hadoop.util.ToolRunner;
import org.sunny.wordcount.DI.MRModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class App {
	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new MRModule());		 
		int exitcode = ToolRunner.run(injector.getInstance(AppRunner.class), args);
		System.exit(exitcode);
	}
}