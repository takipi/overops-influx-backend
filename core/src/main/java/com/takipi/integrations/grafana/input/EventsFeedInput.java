package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.input.ReliabilityReportInput.ReliabilityKpi;

public class EventsFeedInput extends RegressionsInput {
	
	public enum EventType {
		NewError,
		IncError,
		Slowdown
	}
	
	public static final String EVENT_TYPE = "event_type";
	public static final String MESSAGE = "message";

	
	public String kpis;
	
	public String performanceStates;
	
	public Collection<ReliabilityKpi> getKpis() {
		
		if ((kpis == null) || (GrafanaFunction.ALL.equals(kpis))) {
			return Arrays.asList(ReliabilityKpi.values());
		}
		
		List<ReliabilityKpi> result = new ArrayList<ReliabilityKpi>();
		Collection<String> items = getServiceFilters(kpis, null, false);
		
		for (String item : items) {
			ReliabilityKpi kpi = ReliabilityReportInput.getKpi(item);
			result.add(kpi);
		}
		
		return result;
	}
}
