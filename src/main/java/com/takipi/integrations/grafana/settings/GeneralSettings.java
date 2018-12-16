package com.takipi.integrations.grafana.settings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.takipi.integrations.grafana.functions.GrafanaFunction;

public class GeneralSettings {
	
	public boolean group_by_entryPoint;
	public String event_types;
	public int points_wanted;
	public int transaction_points_wanted;
	public String transaction_failures;

	public Collection<String> getDefaultTypes() {
		
		if (event_types == null)
		{
			return Collections.emptyList();
		}
		
		String[] types = event_types.split(GrafanaFunction.GRAFANA_SEPERATOR);
		return Arrays.asList(types);
	}
}
