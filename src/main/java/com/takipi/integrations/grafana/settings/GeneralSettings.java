package com.takipi.integrations.grafana.settings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.takipi.integrations.grafana.functions.GrafanaFunction;

public class GeneralSettings {
	
	public boolean groupByEntryPoint;
	public String eventTypes;

	public Collection<String> getDefaultTypes() {
		
		if (eventTypes == null) {
			return Collections.emptyList();
		}

		String[] types = eventTypes.split(GrafanaFunction.ARRAY_SEPERATOR);
		return Arrays.asList(types);
	}
}
