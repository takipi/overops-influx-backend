package com.takipi.integrations.grafana.input;

public class BaseGraphInput extends EventFilterInput {
	public long interval;
	public int pointsWanted;
	public String seriesName;
}
