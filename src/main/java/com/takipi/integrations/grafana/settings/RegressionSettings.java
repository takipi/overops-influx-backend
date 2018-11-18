package com.takipi.integrations.grafana.settings;

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
}
