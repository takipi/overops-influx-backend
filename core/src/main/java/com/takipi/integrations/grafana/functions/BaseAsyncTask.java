package com.takipi.integrations.grafana.functions;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseAsyncTask implements Callable<Object> {
	
	private static final Logger logger = LoggerFactory.getLogger(BaseAsyncTask.class);
	
	private String oldName;
	private long startTime;
	
	protected void beforeCall() {
		oldName = Thread.currentThread().getName();
		Thread.currentThread().setName(this.toString());
		
		logger.debug("Task {} beforeCall", getClass());
		startTime = System.currentTimeMillis();
	}
	
	protected void afterCall() {
		double sec = (double)(System.currentTimeMillis() - startTime) / 1000;
		logger.debug("Task {} afterCall {} sec", getClass(), sec);
		
		Thread.currentThread().setName(oldName);
	}
}
