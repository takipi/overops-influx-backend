package com.takipi.integrations.grafana.input;

import java.util.Collection;
import java.util.Collections;

public class RegressionsInput extends EventsInput {
	public int activeTimespan;
	public int minBaselineTimespan;
	public int baselineTimespanFactor;
	public int minVolumeThreshold;
	public double minErrorRateThreshold;
	public double regressionDelta;
	public double criticalRegressionDelta;
	public boolean applySeasonality;
	public String criticalExceptionTypes;
	
	public Collection<String> getCriticalExceptionTypes() {

		if (types == null) {
			return Collections.emptySet();
		}

		return getServiceFilters(criticalExceptionTypes, null, false);
	}
}
