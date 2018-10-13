package com.takipi.integrations.grafana.input;

public class BaseVolumeInput extends ViewInput  {
	
	public enum AggregationType {
		sum, avg, count;
	}
	
	public AggregationType type;
}
