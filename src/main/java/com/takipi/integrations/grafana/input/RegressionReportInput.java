package com.takipi.integrations.grafana.input;

public class RegressionReportInput extends RegressionsInput {
	
	public enum ReportMode {
		Applications, Deployments, Tiers;
	}
	
	public ReportMode mode;
	public int limit;
	
	public String thresholds;
	public String postfixes;
}
