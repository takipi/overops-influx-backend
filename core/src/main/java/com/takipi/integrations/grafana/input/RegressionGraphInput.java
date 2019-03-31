package com.takipi.integrations.grafana.input;

/**
 * This function returns a list of time series data depicting volume increases within a target set of events.
 * The algorithm used to decided which events to include is defined the Settings dashboard.
 *    
 * 	Example query:
 * 		regressionGraph({"type":"sum","graphType":"view","volumeType":"all","view":"$view",
 * 		"timeFilter":"$timeFilter","environments":"$environments", 
 * 		"applications":"$applications", "deployments":"$deployments","servers":"$servers",
 * 		"pointsWanted":"$pointsWanted", "types":"$type","limit":3,
 * 		"searchText":"$search", "graphType":"Absolute"})
 */
public class RegressionGraphInput extends GraphLimitInput
{
	/**
	 * Types of  time series returned by this graph. 
	 *
	 */
	public enum RegressionType {
		
		/**
		 * return times series for new issues
		 */
		NewIssues,
		
		/**
		 * return times series for increasing / regressed issues
		 */
		Regressions
	}
	
	/**
	 * Control the type of regressions returned by this graph. Default is Increasing / Regressed events.
	 */
	public RegressionType regressionType;
	
	/**
	 * A string postfix that would be added to the series names of all time series whose increase
	 * is deemed to be severe. For example, a P1 suffix can be added to each time series name whose volume increase
	 * is deemed severe.
	 */
	public String sevSeriesPostfix;
	
	
	/**
	 * The value of each Y point returned by the time series results of this function.
	 */
	public GraphType graphType;
	

	public enum GraphType {
		
		/**
		 * The Y value of each point will denote the volume data of the event at that given point
		 */
		Absolute, 
		
		/**
		 * The Y value of each point will denote the ratio between event volume and calls into the event location at that given point
		 */
		Percentage
	}
	
	public String timeFilterVar;
}
