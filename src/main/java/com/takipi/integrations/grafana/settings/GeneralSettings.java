package com.takipi.integrations.grafana.settings;

import java.util.Collection;

import com.takipi.integrations.grafana.input.EventFilterInput;

public class GeneralSettings {
	
	public boolean groupByEntryPoint;
	public String eventTypes;
	public int pointsWanted;

	public Collection<String> getDefaultTypes() {
		
		return EventFilterInput.toArray(eventTypes);
	}
}
