package com.takipi.integrations.grafana.input;

/**
 * This input us used to populate two functions.
 * 
 * deploymentsAnnotation: this function will produce a list of points each holding the epoch time
 * representing when a deployment was introduced into this environment as well as its name
 * 
 * Example query:
 * 		deploymentsAnnotation({"graphType":"view","volumeType":"all","view":"All Events",
 * 		"timeFilter":"time >= now() - 14d","environments":"$environments", 
 * 		"applications":"$applications", "servers":"$servers","deployments":"$deployments",
 * 		"seriesName":"Times","graphCount":3})
 * 
 * deploymentsGraph: this function will return a set of graphs representing the volume of events
 * within active deployments
 * 
 *  Example query:
 *  		deploymentsGraph({"graphType":"view","volumeType":"all","view":"All Events",
 * 		"timeFilter":"$timeFilter","environments":"$environments", 
 * 		"applications":"$applications", "servers":"$servers","deployments":"$deployments",
 * 		"graphCount":3})
 *
 */
public class DeploymentsGraphInput extends GraphInput{
	
	/**
	 * A limit on the number of graphs or deployment annotations that will be returned by this function
	 */
	public int graphCount;
}
