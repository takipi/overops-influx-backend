package com.takipi.integrations.grafana.input;


/**
 * The base input for all functions that are used to provide data for graph widget.
 *
 */
public abstract class BaseGraphInput extends BaseEventVolumeInput {
	
	/**
	 * If no pointsWanted resolution is specified for the query this value can be used to determine
	 * the number of graph points that would passed to any graph function invoke by this query.
	 * The number of points will be calculated as the the query's time span (t2-t1) /  interval
	 */
	public long interval;
	
	/**
	 The name that would be given to a graph series returned by this query. This may be overriden
	 by derivative graphs.
	 */
	
	public String seriesName;
}
