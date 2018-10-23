package com.takipi.integrations.grafana.input;

import java.util.regex.Pattern;

import com.takipi.integrations.grafana.functions.GroupByFunction.AggregationField;

public class GroupByInput extends VolumeInput {
	
	private Pattern patternFilter;
	
	public AggregationField field;
	public boolean addTags;
	public int limit;
	public String interval;
	public String regexFilter;
	
	public Pattern getPatternFilter() {

		if (regexFilter == null) {
			return null;
		}

		if (patternFilter != null) {
			return patternFilter; 
		}
		
		patternFilter = Pattern.compile(regexFilter);
		
		return patternFilter;
	}
	
	
}
