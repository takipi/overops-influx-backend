package com.takipi.integrations.grafana.settings;

import java.util.Collection;

import com.takipi.integrations.grafana.input.EventFilterInput;

public class GeneralSettings {
	
	public boolean group_by_entryPoint;
	public String event_types;
	public int points_wanted;

	public Collection<String> getDefaultTypes() {
		
		return EventFilterInput.toArray(event_types);
	}
}
