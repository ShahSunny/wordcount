package org.sunny.wordcount.DI;

import org.apache.hadoop.util.Tool;
import org.sunny.wordcount.AppRunner;

import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;

public class MRModule extends AbstractModule {

	@Override
	protected void configure() {
		bindListener(Matchers.any(), new Log4JTypeListener());
		bind(Tool.class).to(AppRunner.class);
	}
}
