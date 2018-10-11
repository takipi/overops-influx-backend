package com.takipi.integrations.grafana.input;

import com.takipi.integrations.grafana.functions.GroupByFunction.AggregationField;

public class GroupByInput extends VolumeInput {
	public AggregationField field;
	public boolean addTags;
	public int limit;
	public String interval;
}
