package com.takipi.integrations.grafana.settings;

import java.util.Arrays;
import java.util.Collection;

import com.takipi.integrations.grafana.functions.GrafanaFunction;

public class RegressionSettings {
	
	public int activeTimespan;
	public int minBaselineTimespan;
	public int baselineTimespanFactor;
	public int minVolumeThreshold;
	public double minErrorRateThreshold;
	public double regressionDelta;
	public double criticalRegressionDelta;
	public boolean applySeasonality;
	public String criticalExceptionTypes;
	
	public String sortOrder;
	public String typeOrder;
	
	public  Collection<String> getCriticalExceptionTypes() {
		
		if (criticalExceptionTypes == null) {
			return null;
		}
		
		return Arrays.asList(criticalExceptionTypes.split(GrafanaFunction.ARRAY_SEPERATOR));
	}
}
