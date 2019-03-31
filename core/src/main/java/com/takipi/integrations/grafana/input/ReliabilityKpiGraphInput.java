package com.takipi.integrations.grafana.input;

import com.takipi.integrations.grafana.util.TimeUtil;

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
		"reportInterval":"24h", "kpi":"$kpi"}) 
		
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
	
	/**
	 * The interval by which to group data (e.g. 1d, 7d,..), default = Day
	 */
	public TimeUtil.Interval reportInterval;
	
	public TimeUtil.Interval getReportInterval() {
		
		if (reportInterval == null) {
			return TimeUtil.Interval.Day;
		}
		
		return reportInterval;
	}
	
	
	/**
	 * Number of points to retrieve when calculating slowdowns
	 */
	public int transactionPointsWanted;
	
	/**
	 * Control whether to aggregate all apps kpis, or break down by the report interval
	 * for trends over time
	 */
	public boolean aggregate;
	
	/**
	 * Control whether too chart scores as 100 - <score>
	 */
	public boolean deductFrom100;
	
}
