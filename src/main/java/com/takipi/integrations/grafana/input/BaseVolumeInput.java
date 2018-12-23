package com.takipi.integrations.grafana.input;

/**
 * Basic input for all functions which returns a single stat volume value
 */
public class BaseVolumeInput extends EventFilterInput  {
	
	/**
	 * The type of volume returned by the query:
	 * 	 sum: return a sum of all values returned by this function
	 *	 avg: return an avg of all values returned by this function
	 *	 count: return a unique count of all values returned by this function
	 */
	public enum AggregationType {
		sum, avg, count;
	}
	
	/**
	 * The type of volume returned by this query
	 */
	public String type;
}
