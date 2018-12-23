package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
import com.takipi.integrations.grafana.settings.GroupSettings.GroupFilter;
import com.takipi.integrations.grafana.util.TimeUtil;

public class EventFilter
{
	
	public static final String CATEGORY_PREFIX = "-";
	public static final String EXCEPTION_PREFIX = "--";
	public static final String TERM = "<term>";
	
	private Collection<String> allowedTypes;
	private Collection<String> types;
	private Collection<String> introducedBy;
	private GroupFilter transactionsFilter;
	private Collection<String> labels;
	private Pattern labelsPattern;
	private Pair<DateTime, DateTime> firstSeen;
	private Categories categories;
	private String searchText;
	private String transactionSearchText;
	private List<String> exceptionTypes;
	private List<String> eventTypes;
	private List<String> categoryTypes;
	
	public static EventFilter of(Collection<String> types, Collection<String> allowedTypes,
			Collection<String> introducedBy, GroupFilter transactionsFilter,
			Collection<String> labels, String labelsRegex, String firstSeen, Categories categories,
			String searchText, String transactionSearchText)
	{
		
		EventFilter result = new EventFilter();
		result.types = types;
		result.allowedTypes = allowedTypes;
		result.introducedBy = introducedBy;
		result.transactionsFilter = transactionsFilter;
		result.labels = labels;
		result.categories = categories;
		
		if (!TERM.equals(searchText))
		{
			result.searchText = searchText;
		}
		
		if (!TERM.equals(transactionSearchText))
		{
			result.transactionSearchText = transactionSearchText;
		}
		
		if (labelsRegex != null)
		{
			result.labelsPattern = Pattern.compile(labelsRegex);
		}
		
		result.exceptionTypes = new ArrayList<String>();
		result.eventTypes = new ArrayList<String>();
		result.categoryTypes = new ArrayList<String>();
		
		if (types != null)
		{
			for (String type : types)
			{
				if (type.startsWith(EXCEPTION_PREFIX))
				{
					result.exceptionTypes.add(type.substring(EXCEPTION_PREFIX.length()));
				} 
				else if (GroupSettings.isGroup(type))
				{
					result.categoryTypes.add(type.substring(CATEGORY_PREFIX.length()));
				}
				else {
					result.eventTypes.add(type);
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
				
				if ((label.equals(GrafanaFunction.HIDDEN)) || (label.equals(GrafanaFunction.RESOVED)))
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
	
	private boolean filterExceptionType(EventResult event) {
		
		if (exceptionTypes.size() == 0)	{
			return false;
		}
		
		if (exceptionTypes.contains(event.name)) {
			return false;
		}
		
		return true;
	}
	
	private boolean filterCategoryType(EventResult event) {
		
		if (categoryTypes.size() == 0) 	{
			return false;
		}
			
		if (event.error_origin != null) {
			Set<String> originLabels = categories.getCategories(event.error_origin.class_name);
			
			if (matchLabels(originLabels)) {
				return false;
			}
		}
		
		if (event.error_location != null) {
			Set<String> locationLabels = categories.getCategories(event.error_location.class_name);
			
			if (matchLabels(locationLabels)) {
				return false;
			}
		}
		
		return true;
	}
	
	private boolean filterEventType(EventResult event) {
		
		if (eventTypes.size() == 0) {
			return false;
		}
		
		if (eventTypes.contains(event.type)) {
			return false;
		}
		
		return true;
	}
	
	private boolean filterType(EventResult event)
	{
		if (filterExceptionType(event)) {
			return true;
		}
		
		if (filterCategoryType(event)) {
			return true;
		}
		
		if (filterEventType(event)) {
			return true;
		}
			
		return false;
	}
	
	private boolean matchLabels(Collection<String> labels) {
		
		if (labels == null) {
			return false;
		}
		
		for (String label : labels)
		{
			for (String type : categoryTypes)
			{
				if (type.equals(label))
				{
					return true;
				}
			}
		}
		
		return false;
	}
	
	private boolean searchLocation(Location location, String s)
	{
		
		if (location == null)
		{
			return false;
		}
		
		if (location.prettified_name.toLowerCase().contains(s))
		{
			return true;
		}
		
		return false;
	}
	
	private boolean searchText(EventResult event)
	{
		
		String s = searchText.toLowerCase();
		
		if (searchLocation(event.error_location, s))
		{
			return true;
		}
		
		if (searchLocation(event.entry_point, s))
		{
			return true;
		}
		
		if ((event.message != null) && (event.message.toLowerCase().contains(s)))
		{
			return true;
		}
		
		return false;
	}
	
	public boolean filterTransaction(EventResult event) {
		
		if (transactionsFilter != null)
		{
			Location entryPoint = event.entry_point;
			
			if ((entryPoint == null) || (entryPoint.class_name == null))
			{
				return true;
			}
			
			if (GrafanaFunction.filterTransaction(transactionsFilter, 
					transactionSearchText, entryPoint.class_name, entryPoint.method_name)) {
				return true;
			}			
		}
		
		return false;
	}
	
	public boolean filter(EventResult event)
	{
		if (event.is_rethrow)
		{
			return true;
		}
		
		if ((!CollectionUtil.safeIsEmpty(allowedTypes)) && (!allowedTypes.contains(event.type)))
		{
			return true;
		}
		
		if (filterTransaction(event)) {
			return true;
		}
			
		if ((types != null) && (!types.isEmpty()))
		{
			if (filterType(event))
			{
				return true;
			}
		}
		
		if ((introducedBy != null) && (!introducedBy.isEmpty()) && (!introducedBy.contains(event.introduced_by)))
		{
			return true;
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
		
		if ((searchText != null) && (!searchText(event)))
		{
			return true;
		}
		
		return false;
	}
	
}
