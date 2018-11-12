package com.takipi.integrations.grafana.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseAsyncTask {
	private static final Logger logger = LoggerFactory.getLogger(BaseAsyncTask.class);
	
	private String oldName;
	
	protected void beforeCall() {
		oldName = Thread.currentThread().getName();
		Thread.currentThread().setName(this.toString());
		
		logger.debug("OO-AS-INFLUX | Task {} beforeCall", getClass());
	}
	
	protected void afterCall() {
		logger.debug("OO-AS-INFLUX | Task {} afterCall", getClass());
		
		Thread.currentThread().setName(oldName);
	}
}
