package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.event.Location;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.CostData;
import com.takipi.integrations.grafana.input.GraphCostLimitInput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.settings.GrafanaSettings;

public class GraphCostPieChartFunction extends CostSplitGraphFunction {

	private static final Logger logger = LoggerFactory.getLogger(GraphCostPieChartFunction.class); 

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new GraphCostPieChartFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return GraphCostLimitInput.class;
		}

		@Override
		public String getName() {
			return "graphCostPie";
		}
	}

	protected class EventData {
		protected EventResult event;
		
		public EventResult getEvent() {
			return event;
		}

		protected EventData(EventResult event) {
			this.event = event;
		}
		
		private boolean equalLocations(Location a, Location b) {
			
			if (a == null) {
				return b == null;
			} 
			
			if (b == null) {
				return false;
			} 
			
			if (!Objects.equal(a.class_name, b.class_name)) {
				return false;
			}

			if (!Objects.equal(a.method_name, b.method_name)) {
				return false;
			}

			if (!Objects.equal(a.method_desc, b.method_desc)) {
				return false;
			}

			return true;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == null ||
				(!(obj instanceof EventData))) {
				return false;
			}
			
			EventData other = (EventData)obj;
			
			if (!Objects.equal(event.type, other.event.type)) {
				return false;
			}
			
			if (!equalLocations(event.error_origin, other.event.error_origin)) {
				return false;
			}
			
			if (!equalLocations(event.error_location, other.event.error_location)) {
				return false;
			}
			
			if (!Objects.equal(event.call_stack_group, other.event.call_stack_group)) {
				return false;
			}
			
			return true;	
		}
		
		@Override
		public int hashCode() {
			
			if (event.error_location == null) {
				return super.hashCode();
			}
			
			return event.error_location.class_name.hashCode();
		}
		
		@Override
		public String toString()
		{
			if (event.entry_point != null) {
				return event.entry_point.class_name;
			}
			
			return super.toString();
		}
	}	
	public GraphCostPieChartFunction(ApiClient apiClient) {
		super(apiClient);
	}

	protected List<EventData> getEventData(ApiClient apiClient, String serviceId, GraphCostLimitInput input, 
			Pair<DateTime, DateTime> timeSpan) {
		
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timeSpan.getFirst(), 
			timeSpan.getSecond(), input.volumeType, input.pointsWanted);
		
		if (eventsMap == null) {
			return Collections.emptyList();
		}
		
		List<EventData> result = new ArrayList<EventData>(eventsMap.size());
		
		for (EventResult event : eventsMap.values()) {
			result.add(new EventData(event));
		}
		
		return result;
	}
	
	@Override
	protected List<GraphSeries> processGraphSeries(Collection<String> serviceIds,
			String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			GraphInput input) {
		GraphCostLimitInput limitInput = (GraphCostLimitInput)input;

		List<EventData> eventDatas = getEventData(apiClient, serviceId, limitInput, timeSpan);

		if (eventDatas == null) {
			return Collections.emptyList();
		}

		EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);

		HashMap<String,Long> runningHitsCostTotal= new HashMap<>();
		
		HashMap<String,Long> eventHitsTotal= new HashMap<>();
		
		CostData costSettings = GrafanaSettings.getData(apiClient, serviceId).cost_calculator;
		
		for (EventData evD : eventDatas) {
			
			EventResult eventData = evD.getEvent();

			if (eventFilter.filter(eventData)) {
				continue;
			}

			if (eventData.stats.hits == 0) {
				continue;
			}


			if ((eventData.type != null) && !eventData.type.trim().isEmpty()) {

				if (costSettings.calculateCost(eventData.type) == .0) {
					continue;
				}

				runningHitsCostTotal.put(eventData.type, eventData.stats.hits + runningHitsCostTotal.getOrDefault(eventData.type, 0l));
				
				if ((eventData.error_location.prettified_name != null) && !eventData.error_location.prettified_name.trim().isEmpty()) {

					String keyEvData = eventData.type + QUALIFIED_DELIM + eventData.error_location.prettified_name;

					eventHitsTotal.put(keyEvData, eventData.stats.hits + eventHitsTotal.getOrDefault(keyEvData, 0l));
		
				}
			}
		}
		
		long intervalLen = timeSpan.getSecond().getMillis() - timeSpan.getFirst().getMillis() + 1L;
		long millisInYr = timeSpan.getSecond().getMillis() - timeSpan.getSecond().minusYears(1).getMillis();
		double intervalFactor = 1.0 * millisInYr / intervalLen;

		SortedMap<String, Double> costPerType = new TreeMap<>();
		
		Double runningCostTotal = .0;
		for (Entry<String, Long> hitGroup : runningHitsCostTotal.entrySet()) {
			Double typeCost = costSettings.calculateCost(hitGroup.getKey());
			runningCostTotal += typeCost * hitGroup.getValue();
			
			costPerType.put(hitGroup.getKey(), typeCost);
		}
		
		Double runningYrlCostTotal = runningCostTotal * intervalFactor;

		final double targetYrlTablelimit = runningYrlCostTotal * Math.min(100.0,Math.max(.01,limitInput.limit)) / 100.0;

		HashMap<String,Double> eventYrlCostTotal = new HashMap<>();
		
		eventHitsTotal.forEach((k,v) -> eventYrlCostTotal.put(k, searchCost(k,costPerType) * v * intervalFactor ));
		
		LinkedHashMap<String,Double> sortedRes = eventYrlCostTotal.entrySet().stream().sorted(Collections.reverseOrder(Entry.comparingByValue())).collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                (e1, e2) -> e1, LinkedHashMap::new));
		
		
		LinkedHashMap<String,Double> trimmedSortedRes = new LinkedHashMap<>();

		{
			double runningTot=.0;
			for (Entry<String, Double> entry : sortedRes.entrySet()) {

				runningTot += entry.getValue();
				trimmedSortedRes.put(entry.getKey(), entry.getValue());
				if (runningTot >= targetYrlTablelimit) {
					break;
				}
			}
			
			if (trimmedSortedRes.size() < sortedRes.size()) {
				trimmedSortedRes.put("CombinedSmallerItems", runningYrlCostTotal - runningTot);
			}
		}
		
		List<GraphSeries> result = new ArrayList<GraphSeries>();

		Long epochTimeNow = Long.valueOf(DateTime.now().getMillis());

		GraphData graphData = new GraphData("");
		graphData.volume = 1;
		for (Entry<String,Double>  res : trimmedSortedRes.entrySet()) {
				
				graphData.points.put(epochTimeNow, res.getValue().longValue());				
				result.add(getGraphSeries(graphData, res.getKey(), input));
		}		
		return result;
	}

	private double searchCost(String eventTypePlusLocation, SortedMap<String, Double> costPerType) {
		String evType = eventTypePlusLocation.substring(0,eventTypePlusLocation.indexOf(QUALIFIED_DELIM));
		SortedMap<String, Double> mySubMap = costPerType.subMap(evType, evType + Character.MAX_VALUE);
		String mySearch = mySubMap.firstKey();
		return mySubMap.get(mySearch);
		
	}

}
