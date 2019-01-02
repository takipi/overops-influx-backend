package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.HashMap;

public class CostCalculatorInput extends EventsInput {
	public Double costHigherThan;
	
	public HashMap<String,Double> costMatrix = new HashMap<>();
	
	public class ReflectionFilter {
		String field;
		String operator;
		String value;
		
		public String getField() {
			return field;
		}

		public String getOperator() {
			return field;
		}
	
		public String getValue() {
			return field;
		}
	}
	
	public ArrayList<ReflectionFilter> reflectionFilters = new ArrayList<>();
	
	public Double tableLimit;
}
