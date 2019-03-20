package com.takipi.integrations.grafana.input;

/**
 * A function charting a target reliability KPI for a selected set of apps. KPIs include:
 * reliability score, error volumes, new errors, increasing errors and more. The out put
 * can be set to chart results over time, grouping them by set report intervals (e.g. 1d, 7d,..)
 * or produce a single average value for each scored application.
 * 
 *  Example query for graph:

	ReliabilityKpiGraph({"volumeType":"all","view":"$view",
		"timeFilter":"$timeFilter","environments":"$environments", 
		"applications":"$applications", "servers":"$servers",
		"types":"$type","limit":"$limit", "pointsWanted":"$pointsWanted", 
		"transactionPointsWanted":"$transactionPointsWanted", 
		"reportInterval":"24h", "kpi":"$kpi"}) *
		
 *Screenshot: https://drive.google.com/file/d/1kSUO1SE5gOqVCYb5eKrsWYxi4PJNbs9J/view?usp=sharing

 */

public class ReliabilityKpiGraphInput extends GraphInput {
	
	/**
	 * The kpi to chart, produced by ReliabilityReportInput.getKpi
	 */
	public String kpi;
	
	/**
	 * The max number of apps to chart
	 */
	public int limit;
	
	public enum ReportInterval {
		Week,
		Day,
		Hour
	}
	
	/**
	 * The interval by which to group data (e.g. 1d, 7d,..)
	 */
	public ReportInterval reportInterval;
	
	public ReportInterval getReportInterval() {
		
		if (reportInterval == null) {
			return ReportInterval.Day;
		}
		
		return reportInterval;
	}
	
	
	/**
	 * Number of points to retrieve when calculating slowdowns
	 */
	public int transactionPointsWanted;
	
	/**
	 * Control whether to aggregate all apps kpis, or break down by the report interval
	 * for trending over time
	 */
	public boolean aggregate;
	
	/**
	 * Control whether too calculate scores as 100 - <X>
	 */
	public boolean deductFrom100;
	
}
