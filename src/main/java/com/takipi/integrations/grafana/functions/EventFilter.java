package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;

import com.takipi.api.client.data.event.Location;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.settings.GroupSettings;
import com.takipi.integrations.grafana.util.TimeUtil;

public class EventFilter
{
	
	public static final String CATEGORY_PREFIX = "-";
	public static final String EXCEPTION_PREFIX = "--";
	public static final String TERM = "<term>";
	
	private Collection<String> allowedTypes;
	private Collection<String> types;
	private Collection<String> introducedBy;
	private Collection<String> transactions;
	private Collection<String> labels;
	private Pattern labelsPattern;
	private Pair<DateTime, DateTime> firstSeen;
	private boolean hasExceptionTypes;
	private boolean hasCategoryTypes;
	private Categories categories;
	private String searchText;
	
	public static EventFilter of(Collection<String> types, Collection<String> allowedTypes,
			Collection<String> introducedBy, Collection<String> transactions,
			Collection<String> labels, String labelsRegex, String firstSeen, Categories categories,
			String searchText)
	{
		
		EventFilter result = new EventFilter();
		result.types = types;
		result.allowedTypes = allowedTypes;
		result.introducedBy = introducedBy;
		result.transactions = transactions;
		result.labels = labels;
		result.categories = categories;
		
		if (!TERM.equals(searchText)) {
			result.searchText = searchText;
		}
		
		if (labelsRegex != null)
		{
			result.labelsPattern = Pattern.compile(labelsRegex);
		}
		
		if (types != null)
		{		
			for (String type : types)
			{
				
				if (type.startsWith(EXCEPTION_PREFIX))
				{
					result.hasExceptionTypes = true;
				}
				
				if (GroupSettings.isGroup(type))
				{
					result.hasCategoryTypes = true;
				}
			}
		}
		
		if (firstSeen != null)
		{
			result.firstSeen = TimeUtil.getTimeFilter(firstSeen);
		}
		
		return result;
	}
	
	private boolean compareLabels(EventResult event)
	{
		
		for (String label : labels)
		{
			for (String eventLabel : event.labels)
			{
				if (label.equals(eventLabel))
				{
					return true;
				}
				
				if ((label.equals(GrafanaFunction.HIDDEN)) 
				|| (label.equals(GrafanaFunction.RESOVED)))
				{
					return false;
				}
			}
		}
		
		return false;
	}
	
	private boolean compareLabelsRegex(EventResult event)
	{
		for (String eventLabel : event.labels)
		{
			Matcher matcher = labelsPattern.matcher(eventLabel);
			
			if (matcher.find())
			{
				return true;
			}
			
		}
		
		return false;
	}
	
	public boolean labelMatches(String label)
	{
		return (labelsPattern == null) || (labelsPattern.matcher(label).find());
	}
	
	private boolean filterType(EventResult event)
	{
		if (event.type.toLowerCase().contains(EXCEPTION_PREFIX))
		{
			if (!types.contains(event.type))
			{
				return true;
			}
			
			if ((hasExceptionTypes) && (!types.contains(event.name)))
			{
				return true;
			}
			
		}
		else if ((hasCategoryTypes) && (event.error_origin != null))
		{
			
			Set<String> labels = categories.getCategories(event.error_origin.class_name);
			
			if (labels != null)
			{
				
				for (String label : labels)
				{
					for (String type : types)
					{
						if (type.contains(label))
						{
							return false;
						}
					}
				}
			}
			
			return true;
		}
		else if (!types.contains(event.type))
		{
			return true;
		}
		
		return false;
	}
	
	private boolean searchLocation(Location location, String s) {
		
		if (location == null) {
			return false;
		}
		
		if (location.prettified_name.toLowerCase().contains(s)) {
			return true;
		}
		
		return false;
	}
	
	private boolean searchText(EventResult event) {
		
		String s = searchText.toLowerCase();
		
		if (searchLocation(event.error_location, s)) {
			return true;
		}
		
		if (searchLocation(event.entry_point, s)) {
			return true;
		}
		
		if ((event.message != null) && (event.message.contains(s))) {
			return true;
		}
		
		return false;
	}
	
	public boolean filter(EventResult event)
	{
		if (event.is_rethrow) {
			return true;
		}
		
		if ((!CollectionUtil.safeIsEmpty(allowedTypes)) && (!allowedTypes.contains(event.type))) {
			return true;
		}
		
		if ((types != null) && (!types.isEmpty()))
		{
			if (filterType(event)) {
				return true;
			}
		}
		
		if ((introducedBy != null) && (!introducedBy.isEmpty()) && (!introducedBy.contains(event.introduced_by)))
		{
			return true;
		}
		
		if ((transactions != null) && (!transactions.isEmpty()))
		{
			
			Location entryPoint = event.entry_point;
			
			if ((entryPoint == null) || (entryPoint.class_name == null))
			{
				return true;
			}
			
			String entryPointName = GrafanaFunction.getSimpleClassName(entryPoint.class_name);
			
			if (!transactions.contains(entryPointName))
			{
				return true;
			}
		}
		
		if ((labels != null) && (!labels.isEmpty()))
		{
			
			if (event.labels == null)
			{
				return true;
			}
			
			if (!compareLabels(event))
			{
				return true;
			}
		}
		
		if (labelsPattern != null)
		{
			
			if (event.labels == null)
			{
				return true;
			}
			
			if (!compareLabelsRegex(event))
			{
				return true;
			}
		}
		
		if (firstSeen != null)
		{
			DateTime eventFirstSeen = TimeUtil.getDateTime(event.first_seen);
			
			boolean inRange =
					(eventFirstSeen.isAfter(firstSeen.getFirst())) && (eventFirstSeen.isBefore(firstSeen.getSecond()));
			
			if (!inRange)
			{
				return true;
			}
		}
		
		if ((searchText != null) && (!searchText(event))) {
			return true;
		}
		
		return false;
	}
	
}
