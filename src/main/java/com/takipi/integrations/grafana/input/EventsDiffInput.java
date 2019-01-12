package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.takipi.integrations.grafana.functions.GrafanaFunction;

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
