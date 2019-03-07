package com.takipi.integrations.grafana.input;

public class FromToInput extends VariableInput {
	
	public String timeFilter;
	
	public enum FromToType {
		From,
		To
	}
	
	public FromToType fromToType; 
	
	public FromToType getFromToType() {
		
		if (fromToType == null) {
			return FromToType.From;
		}
		
		return fromToType;
	}
}
