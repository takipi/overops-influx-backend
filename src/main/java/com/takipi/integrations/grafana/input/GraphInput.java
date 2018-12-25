package com.takipi.integrations.grafana.input;

import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;

/**
 * This function returns the volume of events within a target set of events whose attributes match
 * those defined in the EventFilter section of this funtion's parameters list in time series format
 * which can rendered in a widget.
 * 
 * Example query:
 * 
 * 		graph({"graphType":"view","volumeType":"all","view":"$view","timeFilter":"$timeFilter",
 * 		"environments":"$environments", "applications":"$applications", "servers":"$servers", 
 * 		"deployments":"$deployments","pointsWanted":"$pointsWanted","types":"$type",
 * 		seriesName":"Times", "transactions":"$transactions", "searchText":"$search"})
 * 
 * 	Screenshot: https://drive.google.com/file/d/10yJ3zHbHZiWIfbsd9cePv8oG_EcmGvCk/view?usp=sharing
 *
 */
public class GraphInput extends BaseGraphInput {
	
	/**
	 * The type of event stats used to populate the Y values of points returned by this function. Values:
	 * 	hits, all: the Y values will represent the volume of matching events.
	 *  invocations: the Y values will represent the number of calls into matching events. 
	 */
	public VolumeType volumeType;
	
	/**
	 * If the number of points returned by the query is greater than the pointsWanted parameter then
	 * aggregate the points to match the number specified in pointsWanted. For example, if the pointsWanted
	 * parameter is set to 25, and the OO REST query returned 100 points, then the function will aggregate
	 * each 4 points returned by the OO REST API to one point to ensure that no more than 25 points are returned
	 * in the result time series.
	 */
	public boolean condense;
}

