package com.takipi.integrations.grafana.input;

import java.util.Collection;

/**
 * A function returning time series data of events matching a target filter, broken down by event type.
 * 
 * Example query:
 * 
 * 		typesGraph({"graphType":"view","volumeType":"all","view":"$view","timeFilter":"$timeFilter",
 * 			"environments":"$environments", "applications":"$applications", "servers":"$servers",
 *  			"deployments":"$deployments","pointsWanted":"$pointsWanted","types":"$type",
 *  			"seriesName":"Times", "transactions":"$transactions", "searchText":"$search",
 *  			"defaultTypes":"Logged Error|Uncaught Exception|HTTP Error"})
 */
public class TypesGraphInput extends GraphInput {
	
	/**
	 * A | delimited array of event types for which time series volume data would be split.
	 */
	public String defaultTypes;
	
	public Collection<String> getDefaultTypes() {

		if (!hasFilter(defaultTypes)) {
			return null;
		}

		return getServiceFilters(defaultTypes, null, true);
	}
}
