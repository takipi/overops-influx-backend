package com.takipi.integrations.grafana.input;

import java.util.Collection;

public class CorrelateGraphInput extends TransactionsGraphInput {
	
	public static final String THROUGHPUT_METRIC = "Throughput";
	
	public String metricNames;	
	
	public Collection<String> getMetricNames() {
		return SystemMetricsGraphInput.getMetricNames(metricNames);
	}
}
