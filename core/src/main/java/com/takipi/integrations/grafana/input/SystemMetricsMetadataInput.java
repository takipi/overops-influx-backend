package com.takipi.integrations.grafana.input;

public class SystemMetricsMetadataInput extends BaseEnvironmentsInput {
	
	public static final String[] PRIO_SYSTEM_METRICS = new String[] {
		"class-loading-total-loaded-classes",
		"threads-count",
		"threads-count-waiting"
	};
	
	public boolean addNone;
}
