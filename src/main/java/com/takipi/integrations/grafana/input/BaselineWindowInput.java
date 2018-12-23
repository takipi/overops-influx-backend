package com.takipi.integrations.grafana.input;

/**
 * This query is used to populate a variable holding the value of the current baseline window. 
 * This query will produce one value that can be used for both presentation purposes insidie
 * widget titles as well as to set and Time window overrides for widgets which are used
 * to display graphs that show any regression and slowdown calculations.
 * 
 * Example query:
 * 		baselineWindow({"graphType":"view","view":"$view",
 * 		"timeFilter":"time >= now() - $timeRange","environments":"$environments",
 * 		"applications":"$applications", "servers":"$servers","deployments":"$deployments",
 * 		"baselineOnly":"true"})
 */
public class BaselineWindowInput extends EventFilterInput
{
	/**
	 * Set whether to return the combined value of the active and baseline windows,
	 * or just that of the baseline.
	 */
	public boolean baselineOnly;
}
