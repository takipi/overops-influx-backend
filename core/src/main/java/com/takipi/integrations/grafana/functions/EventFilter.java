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
import com.takipi.api.client.util.settings.GroupSettings;
import com.takipi.api.client.util.settings.GroupSettings.GroupFilter;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.util.TimeUtil;

public class EventFilter
{
	public static final String CRITICAL_EXCEPTIONS = "Critical Exceptions";
	public static final String EXCEPTION_PREFIX = "--";
	public static final String TERM = "<term>";
	public static final String ARCHIVE = "Archive";
	public static final String RESOLVED = "Resolved";
	public static final String APP_CODE = "Application";
	
	private Collection<String> allowedTypes;
	private Collection<String> types;
	private Collection<String> introducedBy;
	private List<Pair<String, String>> eventLocations;
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
	
	public static String toExceptionFilter(String value) {
		
		if (!isExceptionFilter(value)) {
			return EXCEPTION_PREFIX + value;
		} else { 
			return value;
		}
	}
	
	public static boolean isExceptionFilter(String name) {
		return name.startsWith(EXCEPTION_PREFIX);
	}
	
	public static String fromExceptionFilter(String value) {
		
		if ((value == null) || (!isExceptionFilter(value))) {
			return value;
		}
		
		return value.substring(EXCEPTION_PREFIX.length());
	}
	
	
	public static EventFilter of(Collection<String> types, Collection<String> allowedTypes,
			Collection<String> introducedBy, Collection<String> eventLocations, GroupFilter transactionsFilter,
			Collection<String> labels, String labelsRegex, String firstSeen, Categories categories,
			String searchText, String transactionSearchText) {
		
		EventFilter result = new EventFilter();
		result.types = types;
		result.allowedTypes = allowedTypes;
		result.introducedBy = introducedBy;
		
		if (eventLocations != null) {
			
			result.eventLocations = new ArrayList<Pair<String, String>>();
			
			for (String eventLocation : eventLocations) {
				
				String[] eventLocationParts = eventLocation.split(GrafanaFunction.QUALIFIED_DELIM_PATTERN);
						
				if (eventLocationParts.length == 2) {
					result.eventLocations.add(Pair.of(eventLocationParts[0], eventLocationParts[1]));		
				} else {
					result.eventLocations.add(Pair.of(eventLocation, null));		
				}
			}
		}

		result.transactionsFilter = transactionsFilter;
		result.labels = labels;
		result.categories = categories;
		
		if (!TERM.equals(searchText)) {
			result.searchText = searchText;
		}
		
		if (!TERM.equals(transactionSearchText)) {
			result.transactionSearchText = transactionSearchText;
		}
		
		if (labelsRegex != null) {
			result.labelsPattern = Pattern.compile(labelsRegex);
		}
		
		result.exceptionTypes = new ArrayList<String>();
		result.eventTypes = new ArrayList<String>();
		result.categoryTypes = new ArrayList<String>();
		
		if (types != null) {
			
			for (String type : types) {
				
				if (isExceptionFilter(type)) {
					result.exceptionTypes.add(fromExceptionFilter(type));
				} else if (GroupSettings.isGroup(type)) {
					result.categoryTypes.add(GroupSettings.fromGroupName(type));
				} else {
					if (!GrafanaFunction.VAR_ALL.contains(type)) {
						result.eventTypes.add(type);
					}
				}
			}
		}
		
		if (firstSeen != null) {
			result.firstSeen = TimeUtil.getTimeFilter(firstSeen);
		}
		
		return result;
	}
	
	private boolean compareLabels(EventResult event) {
		
		for (String label : labels) {
			
			for (String eventLabel : event.labels) {
				if (label.equals(eventLabel)) {
					return true;
				}
				
				if ((label.equals(GrafanaFunction.HIDDEN)) 
				|| (label.equals(GrafanaFunction.RESOLVED))) {
					return false;
				}
			}
		}
		
		return false;
	}
	
	private boolean compareLabelsRegex(EventResult event) {
		
		for (String eventLabel : event.labels) {
			
			Matcher matcher = labelsPattern.matcher(eventLabel);
			
			if (matcher.find()) {
				return true;
			}
			
		}
		
		return false;
	}
	
	public boolean labelMatches(String label) {
		return (labelsPattern == null) 
			|| (labelsPattern.matcher(label).find());
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
		
		Set<String> originLabels = null;
		Set<String> locationLabels = null;
			
		if (event.error_origin != null) {
			
			originLabels = categories.getCategories(event.error_origin.class_name);
			
			if (matchLabels(originLabels)) {
				return false;
			}
		}
	
		if (event.error_location != null) {
			
			locationLabels = categories.getCategories(event.error_location.class_name);
			
			if (matchLabels(locationLabels)) {
				return false;
			}
		}
		
		boolean hasCategories = (!CollectionUtil.safeIsEmpty(originLabels)) 
							|| (!CollectionUtil.safeIsEmpty(locationLabels));
			
		if ((categoryTypes.contains(APP_CODE)) && (!hasCategories)) {
			return false;
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
	
	private boolean filterType(EventResult event) {
		
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
		
		for (String label : labels)	{
			
			for (String type : categoryTypes) {
				
				if (type.equals(label)) {
					return true;
				}
			}
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
		
		if ((event.message != null) 
		&& (event.message.toLowerCase().contains(s))) {
			return true;
		}
		
		if ((event.introduced_by != null) 
		&& (event.introduced_by.toLowerCase().contains(s))) {
			return true;
		}
		
		return false;
	}
	
	public boolean filterTransaction(EventResult event) {
		
		if (transactionsFilter != null) {
			
			Location entryPoint = event.entry_point;
			
			if ((entryPoint == null) || (entryPoint.class_name == null)) {
				return true;
			}
			
			if (GrafanaFunction.filterTransaction(transactionsFilter, 
					transactionSearchText, entryPoint.class_name, entryPoint.method_name)) {
				return true;
			}			
		}
		
		return false;
	}
	
	private boolean filterEventLocation(EventResult event) {
		
		if (event.error_location == null) {
			return true;
		}
		
		String eventSimpleClass = GrafanaFunction.getSimpleClassName(event.error_location.class_name);
		
		for (Pair<String, String> eventLocation : eventLocations) {
			
			if (!eventLocation.getFirst().equals(eventSimpleClass)) {
				continue;
			}
			
			if ((eventLocation.getSecond() == null) 
			|| (eventLocation.getSecond().equals(event.error_location.method_name))) {
				return false;		
			}
		}
		
		return true;
	}
	
	public boolean filter(EventResult event)
	{
		if (event.is_rethrow) {
			return true;
		}
		
		if (CollectionUtil.safeContains(event.labels, ARCHIVE) ||
			CollectionUtil.safeContains(event.labels, RESOLVED)) {
			return true;
		}
		
		if ((!CollectionUtil.safeIsEmpty(allowedTypes)) 
				&& (!allowedTypes.contains(event.type))) {
			return true;
		}
		
		if (filterTransaction(event)) {
			return true;
		}
			
		if ((types != null) && (!types.isEmpty())) {
			
			if (filterType(event)) {
				return true;
			}
		}
		
		if ((eventLocations != null) && (!eventLocations.isEmpty())) {
			
			if (filterEventLocation(event)) {
				return true;
			}
		} 
		
		if ((introducedBy != null) 
		&& (!introducedBy.isEmpty()) 
		&& (!introducedBy.contains(event.introduced_by))) {
			return true;
		}
		
		if ((labels != null) && (!labels.isEmpty())) {
			
			if (event.labels == null) {
				return true;
			}
			
			if (!compareLabels(event)) {
				return true;
			}
		}
		
		if (labelsPattern != null) {
			
			if (event.labels == null) {
				return true;
			}
			
			if (!compareLabelsRegex(event)) {
				return true;
			}
		}
		
		if (firstSeen != null) {
			
			DateTime eventFirstSeen = TimeUtil.getDateTime(event.first_seen);
			
			boolean inRange = (eventFirstSeen.isAfter(firstSeen.getFirst())) 
							&& (eventFirstSeen.isBefore(firstSeen.getSecond()));
			
			if (!inRange) {
				return true;
			}
		}
		
		if ((searchText != null) && (!searchText(event))) {
			return true;
		}
		
		return false;
	}
}
