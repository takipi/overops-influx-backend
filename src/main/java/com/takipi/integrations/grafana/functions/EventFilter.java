package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;

import com.takipi.api.client.data.event.Location;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.util.TimeUtil;

public class EventFilter {

	public static final String CATEGORY_PREFIX = "-";
	public static final String TYPE_PREFIX = "--";

	private Collection<String> types;
	private Collection<String> introducedBy;
	private Collection<String> transactions;
	private Collection<String> labels;
	private Pattern labelsPattern;
	private Pair<DateTime, DateTime> firstSeen;
	private boolean hasExceptionTypes;
	private boolean hasCategoryTypes;
	private Categories categories;

	public static EventFilter of(Collection<String> types, Collection<String> introducedBy,
			Collection<String> transactions, Collection<String> labels, 
			String labelsRegex, String firstSeen, Categories categories) {

		EventFilter result = new EventFilter();
		result.types = types;
		result.introducedBy = introducedBy;
		result.transactions = transactions;
		result.labels = labels;
		result.categories = categories;

		if (labelsRegex != null) {
			result.labelsPattern = Pattern.compile(labelsRegex);
		}

		if (types != null) {

			for (String type : types) {

				if (type.startsWith(TYPE_PREFIX)) {
					result.hasExceptionTypes = true;
				}

				if (type.startsWith(CATEGORY_PREFIX)) {
					result.hasCategoryTypes = true;
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
		return (labelsPattern == null) || (labelsPattern.matcher(label).find());
	}
	
	public boolean filter(EventResult event) {
		
		if ((types != null) && (!types.isEmpty())) {

			if (event.type.toLowerCase().contains(TYPE_PREFIX)) {

				if (!types.contains(event.type)) {
					return true;
				}

				if ((hasExceptionTypes) && (!types.contains(event.name))) {
					return true;
				}
				
			} else if ((hasCategoryTypes) && (event.error_origin != null)) {

				Set<String> labels = categories.getCategories(event.error_origin.class_name);

				if (labels != null) {
				
					for (String label : labels) {
						for (String type : types) {
							if (type.contains(label)) {
								return false;
							}
						}
					}
				}
				
				return true;
			} else if (!types.contains(event.type)) {
				return true;
			}
		}

		if ((introducedBy != null) && (!introducedBy.isEmpty()) 
		&& (!introducedBy.contains(event.introduced_by)))

		{
			return true;
		}

		if ((transactions != null) && (!transactions.isEmpty())) {
			
			Location entryPoint = event.entry_point;
			
			if ((entryPoint == null) || (entryPoint.class_name == null)) {
				return true;
			}
			
			String entryPointName = GrafanaFunction.getSimpleClassName(entryPoint.class_name);

			if (!transactions.contains(entryPointName)) {
				return true;
			}
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

		return false;
	}

}
