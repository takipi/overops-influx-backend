package com.takipi.integrations.grafana.settings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.takipi.integrations.grafana.functions.GrafanaFunction;

public class GraphSettings {
	public String defaultTypes;

	public Collection<String> getDefaultTypes() {
		
		if (defaultTypes == null) {
			return Collections.emptyList();
		}

		String[] types = defaultTypes.split(GrafanaFunction.ARRAY_SEPERATOR);
		return Arrays.asList(types);
	}
}
