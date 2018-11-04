package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;

import com.takipi.api.client.result.event.EventResult;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.util.TimeUtil;

public class EventFilter {
	
	private Collection<String> types;
	private Collection<String> introducedBy;
	private Collection<String> transactions;
	private Collection<String> labels;
	private Pattern labelsPattern;
	private Pair<DateTime, DateTime> firstSeen;

	public static EventFilter of(Collection<String> types, Collection<String> introducedBy,
			Collection<String> transactions, Collection<String> labels, String labelsRegex,
			String firstSeen) {

		EventFilter result = new EventFilter();
		result.types = types;
		result.introducedBy = introducedBy;
		result.transactions = transactions;
		result.labels = labels;

		if (labelsRegex != null) {
			result.labelsPattern = Pattern.compile(labelsRegex);
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
		
		if ((types != null) && (!types.isEmpty()) && (!types.contains(event.type.toLowerCase()))) {
			return true;
		}

		if ((introducedBy != null) && (!introducedBy.isEmpty()) 
		&& (!introducedBy.contains(event.introduced_by))) {
			return true;
		}

		if ((transactions != null) && (!transactions.isEmpty())) {
			String entryPoint = GrafanaFunction.getSimpleClassName(event.entry_point.class_name);

			if (!transactions.contains(entryPoint)) {
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
