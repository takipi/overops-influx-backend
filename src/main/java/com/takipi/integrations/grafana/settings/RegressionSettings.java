package com.takipi.integrations.grafana.settings;

import java.util.Arrays;
import java.util.Collection;

import com.takipi.integrations.grafana.functions.GrafanaFunction;

public class RegressionSettings {
	
	public int active_timespan;
	public int min_baseline_timespan;
	public int baseline_timespan_factor;
	
	public int error_min_volume_threshold;
	public double error_min_rate_threshold;
	public double error_regression_delta;
	public double error_critical_regression_delta;
		
	public boolean apply_seasonality;
	public String critical_exception_types;
	
	public String sort_order;
	public String type_order;
	
	public  Collection<String> getCriticalExceptionTypes() {
		
		if (critical_exception_types == null) {
			return null;
		}
		
		Collection<String> result = Arrays.asList(critical_exception_types.split(GrafanaFunction.ARRAY_SEPERATOR));
		
		return result;
	}
}
