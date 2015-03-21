package org.sunny.wordcount.DI;

import java.lang.reflect.Field;

import org.apache.log4j.Logger;

import com.google.inject.TypeLiteral;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

public class Log4JTypeListener implements TypeListener {

	public <I> void hear(TypeLiteral<I> typeLiteral, TypeEncounter<I> typeEncounter) {
		Class<?> clazz = typeLiteral.getRawType();
	      while (clazz != null) {
	        for (Field field : clazz.getDeclaredFields()) {
	          if (field.getType() == Logger.class &&
	            field.isAnnotationPresent(InjectLogger.class)) {
	        	  System.out.println("From Log4JTypeListener " + InjectLogger.class.toString());
	            typeEncounter.register(new Log4JMembersInjector<I>(field));
	          }
	        }
	        clazz = clazz.getSuperclass();
	      }
	}

}
