package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.util.ArrayUtil;

/**
 * This function populates a variable containing all the event types available to the user to filter 
 * events by. The functions returns a list containing three types of event categorizations:
 * 		1. A list of all event types available to the user to filter by. These could be provided by the function's
 * 		types variable, or if left null will be read from the "event_types" value under the "general"
 * 		section of the Settings dashboard.
 * 		2. A list of all code tiers to which events active in the last 7 days belong. These are preceded 
 * 		by a "-" prefix.
 * 		3. A list of all exception simple name (e.g. NullPointerException) that match those of events
 * 		active in the last 7 days. These values are preceded by a "--" prefix.
 *  
 * Example query:
 * 		eventTypes({"environments":"$environments", "view":"$view"})  	
 *
 */
public class EventTypesInput extends ViewInput {
	
	/**
	 * An optional list of values used to specify the event types (e.g Logged Error) available
	 * to the user to filter by. If this value is left null, the list of allowed event types will be read from the
	 * "event_types" value under the "general"
	 *  section of the Settings dashboard.
	 *  
	 *  Example query:
	 *  		eventTypes({"environments":"$environments", "view":"All Events"})
	 *  
	 *  Screenshot: https://drive.google.com/file/d/1bCrsjOcPZrht7Z78qi4XRHr3O4xF7CoF/view?usp=sharing
	 */
	
	/**
	 * A | delimited list of the events type to add to the list. If no value is provided,
	 * the event_types property is used from theSettings dashboard. 
	 */
	public String types; 
	
	/**
	 * if set to true only populate values seen in events first seen in the last week
	 */
	public boolean newOnly;

	/**
	 * The different event types that will be added to the list
	 */
	public enum EventTypes {
		
		/**
		 * Populate the different available event types
		 */
		EventTypes,
		
		/**
		 * Populate the different exception types available to the user
		 */
		ExceptionTypes,
		
		/**
		 * Populate the different code tiers available to the user
		 */
		Tiers
	}
	
	/**
	 * A comma delimited list of the different event types to populate in the list. If no value is specified,
	 * all event types are added
	 */
	public String eventTypes;
	
	public Collection<String> getTypes() {
		
		if (types == null) {
			return Collections.emptyList();
		}

		return Arrays.asList(ArrayUtil.safeSplitArray(types, GrafanaFunction.GRAFANA_SEPERATOR, false));
	}
	
	public Collection<EventTypes> getEventTypes()
	{	
		if (eventTypes == null)
		{
			return Arrays.asList(EventTypes.values());
		}
		
		String[] parts = eventTypes.split(GrafanaFunction.ARRAY_SEPERATOR);
		Collection<EventTypes> result = new ArrayList<EventTypes>(parts.length);
		
		for (String part : parts)
		{
			EventTypes type = EventTypes.valueOf(part);
			
			if (type != null)
			{
				result.add(type);
			}
		}
		
		return result;
		
	}
}

