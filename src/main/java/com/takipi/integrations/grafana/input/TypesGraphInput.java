package com.takipi.integrations.grafana.input;

import java.util.Collection;

public class TypesGraphInput extends GraphInput {
	public String defaultTypes;
	
	public Collection<String> getDefaultTypes() {

		if (!hasFilter(defaultTypes)) {
			return null;
		}

		return getServiceFilters(defaultTypes, null, true);
	}
}
