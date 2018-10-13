package com.takipi.integrations.grafana.functions;

import java.util.Collection;

import com.takipi.common.api.result.event.EventResult;

public class EventFilter {
	private Collection<String> types;
	private Collection<String> introducedBy;
	private Collection<String> transactions;

	public static EventFilter of(Collection<String> types, Collection<String> introducedBy,
			Collection<String> transactions) {
		
		EventFilter result = new EventFilter();
		result.types = types;
		result.introducedBy = introducedBy;
		result.transactions = transactions;
		
		return result;
	}

	public boolean filter(EventResult event) {
		if ((types != null) && (!types.isEmpty()) && (!types.contains(event.name))) {
			return true;
		}

		if ((introducedBy != null) && (!introducedBy.isEmpty()) && (!introducedBy.contains(event.introduced_by))) {
			return true;
		}
	

		if ((transactions != null) && (!transactions.isEmpty())) {
			String entryPoint = GrafanaFunction.getSimpleClassName(event.entry_point.class_name);
	
			if (!transactions.contains(entryPoint)) {
				return true;
			}
		}

		return false;
	}

}
