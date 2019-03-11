package com.takipi.integrations.grafana.input;

public class TimeFilterInput extends VariableInput {
	public String timeFilter;
	public String limit;
	public boolean rangeOnly;
	public String rangePrefix;
}
