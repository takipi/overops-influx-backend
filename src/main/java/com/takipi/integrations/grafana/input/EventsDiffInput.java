package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.takipi.integrations.grafana.functions.GrafanaFunction;

/**
 * A function that compares two different combinations of a application/deployment/server
 * filter set, returning events that are either new or have regressed between the different
 * filters. This is especially useful for comparing errors between to different versions.
 * 
 *  Example query:
 *  
 *  eventsDiff({"fields":"link,type,entry_point,introduced_by,jira_issue_url,
 *  		id,rate_desc,diff_desc,diff,message,error_location,stats.hits,rate,
 *  		first_seen,jira_state","view":"$view","timeFilter":"$timeFilter",
 *  		"environments":"$environments","applications":"$applications",
 *  		"servers":"$servers","deployments":"$deployments","volumeType":"all",
 *  		"maxColumnLength":80, "types":"$type","pointsWanted":"$pointsWanted",
 *  		"transactions":"$transactions", "searchText":"$search",
 *  		"compareToApplications":"$compareToApplications", 
 *  		"compareToDeployments":"$compareToDeployments",
 * 		"compareToServers":"$compareToServers", "diffTypes":"Increasing"})
 *  
 *  Screenshot: https://drive.google.com/file/d/1l6ARZfTCR3UfBh649uO_VhAipeX8njZ4/view?usp=sharing
 *
 */
public class EventsDiffInput extends EventsInput
{
	/**
	 * A comma delimited array of application names to compare against
	 */
	public String compareToApplications;
	
	/**
	 * A comma delimited array of server names to compare against
	 */
	
	public String compareToServers;
	
	/**
	 * A comma delimited array of deployment names  to compare against
	 */
	public String compareToDeployments;
	
	/**
	 * Additional available field describing the diff for the current even from the prev release.
	 */
	
	/**
	 * The types of available diff events
	 */
	public enum DiffType {
		
		/**
		 * Event is new in target release.
		 */
		New,
		
		/**
		 * Event is increased in target release.
		 */
		Increasing
	}
	
	/**
	 * Comma delimited list of diff types to display
	 */
	public String diffTypes; 
	
	/**
	 * A string describing a time unit to be used as an offset when comparing the filters.
	 * For example setting 24h, will compare the current time window with a time window 
	 * stating and ending 24h before
	 */
	
	public String timeDiff;
	
	/**
	 * Define the top number of results
	 */
	public String limit;
	
	/**
	 * Additional available fields
	 */
	public static final String DIFF = "diff";
	public static final String DIFF_DESC = "diff_desc";
	
	public Collection<DiffType> getDiffTypes()
	{
		
		if (diffTypes == null)
		{
			return Collections.emptyList();
		}
		
		String[] parts = diffTypes.split(GrafanaFunction.ARRAY_SEPERATOR);
		Collection<DiffType> result = new ArrayList<DiffType>(parts.length);
		
		for (String part : parts)
		{
			DiffType type = DiffType.valueOf(part);
			
			if (type != null)
			{
				result.add(type);
			}
		}
		
		return result;
		
	}
}
