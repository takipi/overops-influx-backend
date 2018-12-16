package com.takipi.integrations.grafana.settings;

public class SlowdownSettings
{
	public int active_invocations_threshold;
	public int baseline_invocations_threshold;
	public double over_avg_slowing_percentage;
	public double over_avg_critical_percentage;
	public double std_dev_factor;
}
