package com.takipi.integrations.grafana.functions;

public class BaseAsyncTask {
	
	private String oldName;
	
	protected void beforeCall() {
		oldName = Thread.currentThread().getName();
		Thread.currentThread().setName(this.toString());
	}
	
	protected void afterCall() {
		Thread.currentThread().setName(oldName);
	}
}
