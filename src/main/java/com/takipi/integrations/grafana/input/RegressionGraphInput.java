package com.takipi.integrations.grafana.input;

public class RegressionGraphInput extends GraphLimitInput
{
	public String sevSeriesPostfix;
	public GraphType graphType;
	public String timeOverride;
	
	public enum GraphType {
		Abosolute, Percentage
	}
}
