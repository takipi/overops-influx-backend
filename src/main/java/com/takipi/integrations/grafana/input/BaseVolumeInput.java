package com.takipi.integrations.grafana.input;

public class BaseVolumeInput extends EventFilterInput  {
	
	public enum AggregationType {
		sum, avg, count;
	}
	
	public String type;
}
