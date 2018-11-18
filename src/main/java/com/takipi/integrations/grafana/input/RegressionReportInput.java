package com.takipi.integrations.grafana.input;

public class RegressionReportInput  extends RegressionsInput {
	
	public enum Mode {
		Applications, Deployments, Tiers;
	}
	
	public Mode mode;
	public boolean graph;
	public int limit;
}
