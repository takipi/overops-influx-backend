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
	 * 
	 * The available KPI types to chart
	 */
	public enum ReliabilityKpi {
		
		/**
		 * Chart the number of new errors introduced into each target app
		 */
		NewErrors,
		
		/**
		 * Chart the number of severe new errors introduced into each target app
		 */
		SevereNewErrors,
		
		/**
		 * Chart the number of error increases introduced into each target app
		 */
		IncreasingErrors,
		
		/**
		 * Chart the number of severe error increases introduced into each target app
		 */
		SevereIncreasingErrors,
		
		/**
		 * Chart the number of slowdowns introduced into each target app
		 */
		Slowdowns,
		
		/**
		 * Chart the number of severe slowdowns introduced into each target app
		 */
		SevereSlowdowns,
		
		/**
		 * Chart the number of event volume for each app
		 */
		ErrorVolume,
		
		/**
		 * Chart the number of unique events for each app
		 */
		ErrorCount,
		
		/**
		 * Chart the event volume / invocations ratio for each app
		 */
		ErrorRate,
		
		/**
		 * Chart the overall reliability score of each app
		 */
		Score
	}
	
	/**
	 * The kpi to chart.
	 */
	public String kpi;
	
	public ReliabilityKpi getKpi() {
		
		if ((kpi ==  null) || (kpi.length() == 0)) {
			return ReliabilityKpi.ErrorVolume;
		}
		
		ReliabilityKpi result = ReliabilityKpi.valueOf(kpi.replace(" ", ""));
		
		return result;
	}
	
	/**
	 * The max number of apps to chart
	 */
	public int limit;
	
	/**
	 * The interval by which to group data (e.g. 1d, 7d,..)
	 */
	public String reportInterval;
	
	/**
	 * Number of points to retrieve when calculating slowdowns
	 */
	public int transactionPointsWanted;
	
	/**
	 * Control whether to aggregate all apps kpis, or break down by the report interval
	 * for trending over time
	 */
	public boolean aggregate;
	
}
