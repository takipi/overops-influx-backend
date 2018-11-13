package com.takipi.integrations.grafana.input;

import java.util.Collection;
import java.util.Collections;

import com.takipi.integrations.grafana.functions.EventFilter;

public class EventFilterInput extends ViewInput {
	public String introducedBy;
	public String types;
	public String labels;
	public String labelsRegex;
	public String firstSeen;
	public int pointsWanted;

	public boolean hasEventFilter() {
		
		if ((introducedBy != null) && (introducedBy.length() > 0)) {
			return true;
		}
		
		if ((types != null) && (types.length() > 0)) {
			return true;
		}
		
		if ((labels != null) && (labels.length() > 0)) {
			return true;
		}
		
		if ((labelsRegex != null) && (labelsRegex.length() > 0)) {
			return true;
		}
		
		if ((firstSeen != null) && (firstSeen.length() > 0)) {
			return true;
		}
		
		if ((transactions != null) && (transactions.length() > 0)) {
			return true;
		}
		
		return false;
	}
	
	public boolean hasIntroducedBy() {
		return hasFilter(introducedBy);
	}

	public Collection<String> getIntroducedBy(String serviceId) {

		if (introducedBy == null) {
			return Collections.emptySet();
		}

		return getServiceFilters(introducedBy, serviceId, true);
	}

	public Collection<String> getTypes() {

		if (!hasFilter(types)) {
			return null;
		}

		return getServiceFilters(types, null, true);
	}

	public Collection<String> geLabels(String serviceId) {

		if (labels == null) {
			return Collections.emptySet();
		}

		return getServiceFilters(labels, serviceId, true);
	}

	public EventFilter getEventFilter(String serviceId) {
		return EventFilter.of(getTypes(), getIntroducedBy(serviceId), getTransactions(serviceId), geLabels(serviceId),
				labelsRegex, firstSeen);
	}

}
