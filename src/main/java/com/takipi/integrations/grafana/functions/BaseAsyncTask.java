package com.takipi.integrations.grafana.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseAsyncTask {
	
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
		double sec = (System.currentTimeMillis() - startTime) / 1000;
		logger.debug("Task {} afterCall {} sec", getClass(), sec);
		
		Thread.currentThread().setName(oldName);
	}
}
