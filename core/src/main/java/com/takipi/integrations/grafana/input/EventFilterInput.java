package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.functions.EventFilter;
import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.input.RegressionSettings;

/**
 * The base function input used to include / exclude event objects matching a specific criteria
 * the could be selected by the user. 
 *
 */
public abstract class EventFilterInput extends ViewInput
{
	/**
	 * An optional comma delimited string array of deployment names to select all objects introduced by 
	 * a target set of deployments
	 */
	public String introducedBy;
	
	/**
	 * An optional comma delimited array of type identifier to select all object of a specific type
	 * Each value can be one of the following :
	 * 		1.The type name of the object. Possible values include: "Logged Error, "Logged Warning,
	 * 		2. "Uncaught Exception, Caught Exception, "Swallowed Exception", "Timer". 
	 * 		The simple name of an exception class. Must be preceded by a double dash ('--'). 
	 * 		3. The name of a category as defined in https://git.io/fpPT0, or as added via the Settings dashboard.
	 * 		Must be preceded by a dash ('-').
	 * 
	 * 		The logical relationship between the values in the types array is a logical AND which means
	 * 		an object must pass all of the selectors to specified to be selected. 
	 */
	public String types;
	
	/** 
	 * An optional comma delimited list of labels names the event must match to be selected. It is 
	 * enough for the event to match at least one of the labels specified to be selected.
	 */
	public String labels;

	
	/**
	 * An optional string value list containing a value in the format of class.method.
	 * If the value is matched against the error_location of a target event, it is selected.
	 * 
	 */
	public String eventLocations;


	/**
	 * An optional regex pattern applied to each of the target event's labels. If one of the labels
	 * match the regex pattern the event is selected.
	 */
	public String labelsRegex;
	
	/**
	 * An optional String value containing a ISO 8601 date time value. For the object to be selected
	 * its first_seen attribute must be later than the date specified.
	 */
	public String firstSeen;
	
	/**
	 * An optional string value containing a term that must be contained in a target event's
	 * error location (class or method name), entry point (class or method name), message or type for it to be selected. The value '<term>' is ignored.
	 * 
	 */
	public String searchText;
	
	/**
	 * An optional list of event types (as specified by the types array, excluding exception names and categories)
	 * that a target event must match to be selected. This filter is applied as a logical AND to
	 * that specified by the types filter.
	 */
	public String allowedTypes;
	
	/**
	 * An optional string value that must be contained in the event's entry point (class or name) for it to be selected.
	 */
	public String transactionSearchText;
	
	public String getSearchText() {
		
		if ((searchText == null) || (searchText.equals(EventFilter.TERM)) ) {
			return null;
		}
		
		return searchText;
		
	}
	
	public boolean hasEventFilter()
	{
		
		if ((introducedBy != null) && (introducedBy.length() > 0))
		{
			return true;
		}
		
		if ((types != null) && (types.length() > 0))
		{
			return true;
		}
		
		if ((labels != null) && (labels.length() > 0))
		{
			return true;
		}
		
		if ((labelsRegex != null) && (labelsRegex.length() > 0))
		{
			return true;
		}
		
		if ((firstSeen != null) && (firstSeen.length() > 0))
		{
			return true;
		}
		
		if (hasTransactions())
		{
			return true;
		}
		
		return false;
	}
	
	public boolean hasIntroducedBy()
	{
		return hasFilter(introducedBy);
	}
	
	public Collection<String> getIntroducedBy(String serviceId)
	{
		
		if (introducedBy == null)
		{
			return Collections.emptySet();
		}
		
		return getServiceFilters(introducedBy, serviceId, true);
	}
	
	public Collection<String> getTypes(ApiClient apiClient, String serviceId) {
		return getTypes(apiClient, serviceId, true);
	}

	public Collection<String> getTypes(ApiClient apiClient, String serviceId, boolean expandCriticalTypes)
	{
		
		if (!hasFilter(types))
		{
			return null;
		}
		
		String value = types.replace(GrafanaFunction.ARRAY_SEPERATOR_RAW,
				GrafanaFunction.GRAFANA_SEPERATOR_RAW);
		
		List<String> result = new ArrayList<String>();
		Collection<String> types = getServiceFilters(value, null, true);
		
		for (String type : types) {
			
			if ((expandCriticalTypes) && (EventFilter.CRITICAL_EXCEPTIONS.equals(type))) {
				RegressionSettings regressionSettings = GrafanaSettings.getData(apiClient, serviceId).regression;
				
				if (regressionSettings != null) {
					Collection<String> criticalExceptionTypes = regressionSettings.getCriticalExceptionTypes();
					
					for (String criticalExceptionType : criticalExceptionTypes) {
						result.add(EventFilter.toExceptionFilter(criticalExceptionType));
					}
				}
			} else {
				result.add(type);
			}
		}
		
		return result;
	}
		
	public Collection<String> geLabels(String serviceId)
	{
		
		if (labels == null)
		{
			return Collections.emptySet();
		}
		
		return getServiceFilters(labels, serviceId, true);
	}	
}
