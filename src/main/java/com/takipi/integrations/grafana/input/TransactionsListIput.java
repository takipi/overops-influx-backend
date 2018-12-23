package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.takipi.api.client.util.performance.calc.PerformanceState;
import com.takipi.integrations.grafana.functions.GrafanaFunction;

public class TransactionsListIput extends BaseGraphInput {
	
	public enum RenderMode
	{
		SingleStat,
		Grid
	}
	
	public String fields;
	public RenderMode renderMode;
	public String singleStatFormat;
	
	public String performanceStates;
	
	public static Collection<PerformanceState> getStates(String performanceStates) {
		
		List<PerformanceState> result = new ArrayList<PerformanceState>();
		
		if (performanceStates != null) {
			String[] parts = performanceStates.split(GrafanaFunction.GRAFANA_SEPERATOR);
			
			for (String part : parts) {
				PerformanceState state = PerformanceState.valueOf(part);
				
				if (state == null) {
					throw new IllegalStateException("Unsupported state " + part + " in " + performanceStates);
				}
				
				result.add(state);
			}
		} else {
			for (PerformanceState state : PerformanceState.values()) {
				result.add(state);
			}
		}
		
		return result;
	}
	
}
