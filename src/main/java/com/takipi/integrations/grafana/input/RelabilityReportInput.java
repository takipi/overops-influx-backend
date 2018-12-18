package com.takipi.integrations.grafana.input;

public class RelabilityReportInput extends RegressionsInput {
	
	public enum ReportMode {
		Applications, Deployments, Tiers;
	}
	
	public ReportMode mode;
	public int limit;
	
	public String thresholds;
	public String postfixes;
	
	public int transactionPointsWanted;
	
	public String sevAndNonSevFormat;
	public String sevOnlyFormat;
}
