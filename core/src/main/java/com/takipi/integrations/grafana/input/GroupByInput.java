package com.takipi.integrations.grafana.input;

import java.util.regex.Pattern;

/**
 * This function enables the user return event volume data that is "grouped" by a certain attribute
 * of the events selected.
 * 
 * Example query:
 * 	 	groupBy({"type":"sum","field":"introduced_by","limit":5,"volumeType":"all",
 * 		"view":"All Events","timeFilter":"$timeFilter","environments":"$environments",
 * 		"applications":"$applications","servers":"$servers",
 * 		"types":"Logged Error|Swallowed Exception|Logged Warning|Uncaught Exception|HTTP Error",
 * 		"interval":"$__interval"})
 * 
 *  Screenshot: https://drive.google.com/file/d/1iucN3zx2MSubKZx5I7UuJPs6dfB7P82F/view?usp=sharing
 */
public class GroupByInput extends VolumeInput {
	
	/**
	 * Control by which to group events matching the function's filter.
	 *
	 */
	public enum AggregationField {
		/**
		 * Group by event types (e.g. Logged Error,Swallowed Exception,..)
		 */
		type, 
		
		/**
		 * Group by event names (e.g. Logged Error, NullPointerException,..)
		 */
		name, 
		
		/**
		 * Group by event locations (e.g classA.foo1, classB.foo2,..)
		 */
		location, 
		
		/**
		 * Group by event entry points (e.g ServeletA.doGet, ServeletB.doGet,..)
		 */
		entryPoint, 
		
		/**
		 * Group by event labels (e.g. JIRA, Resolved, AWS,..)
		 */
		label, 
		
		/**
		 * Group events by the deployment in which they were introduced (e.g. v1.1, v1.2, v1.3,..)
		 */
		introduced_by, 
		
		/**
		 * Group event volume by application (e.g. Broker, Client, Bidder,..)
		 */
		application, 
		
		/**
		 * Group event volume by server or server group (e.g. AWS-EAST, AWS-WEST, Prod-Server1,..)
		 */
		server, 
		
		/**
		 * Group events by the deployment in they occurred (e.g. v1.1, v1.2, v1.3,..). An event may be introduced
		 * in a previous deployment (e.g. v1.3 which will match its introduced_by attribute) but surge due to
		 * code or infrastructure changes in a subsequent deployment (e.g v1.5).
		 */
		deployment;
	}
	
	/**
	 * The attribute by which to group volumes, can be any one of the values defined in the AggregationField enum. 
	 */
	public AggregationField field;
		
	/**
	 * Control whether to create individual columns for each of the volume types returned by this query.
	 */
	public boolean addTags;
	
	/**
	 * limit the number of applications / deployments / servers by which event volume is grouped to
	 * the most recent <limit> deployments, or the the applications / servers which have the most event volume.
	 */
	public int limit;
	
	/**
	 * Set the time interval by which points in the returned time series are spaced. For example,
	 * an interval of 1d, means that a points containing the aggregated volume will be returned for each
	 * day within the time span (defined by the timeFilter parameter) executes.
	 */
	public String interval;
	
	/** 
	 * A regex pattern used to filter in / out any value returned as the row name of each of the returned groups.
	 * For example, if the function is used to group event volume by application and the invoker 
	 * would like to return rows for applications whose name starts with "microservice", the ^microservice regex
	 * can be provided. The same can be used for labels, deployments, servers and more.
	 */
	public String regexFilter;
	
	public Pattern getPatternFilter() {

		if (regexFilter == null) {
			return null;
		}

		if (patternFilter != null) {
			return patternFilter; 
		}
		
		patternFilter = Pattern.compile(regexFilter);
		
		return patternFilter;
	}
	
	private Pattern patternFilter;

}
