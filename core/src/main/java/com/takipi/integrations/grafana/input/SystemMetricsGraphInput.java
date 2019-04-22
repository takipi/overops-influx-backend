package com.takipi.integrations.grafana.input;

import java.util.Collection;

public class SystemMetricsGraphInput extends BaseGraphInput {
	
	public String metricNames;
	
	public Collection<String> getMetricNames() {
		return getMetricNames(metricNames);
	}

	public static Collection<String> getMetricNames(String metricNames) {
		return getServiceFilters(metricNames, null, false);
	}
}
