package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.EventsFunction.EventData;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionData;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.input.RegressionGraphInput;
import com.takipi.integrations.grafana.input.RegressionGraphInput.GraphType;
import com.takipi.integrations.grafana.input.RegressionGraphInput.RegressionType;
import com.takipi.integrations.grafana.util.TimeUtil;

public class RegressionGraphFunction extends LimitGraphFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new RegressionGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return RegressionGraphInput.class;
		}

		@Override
		public String getName() {
			return "regressionGraph";
		}
	}

	public RegressionGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	private EventResult getEvent(List<EventData> eventDatas, String id) {
		
		for (EventData eventData : eventDatas) {
			if (eventData.event.id.equals(id)) {
				return eventData.event;
			}
			
			RegressionData regData = (RegressionData)eventData;
			
			if (regData.mergedIds.contains(id)) {
				return eventData.event;
			}
		}
		
		return null;
	}
	
	private void appendGraphToMap(Map<String, GraphData> graphsData, 
			List<EventData> eventData, Graph graph, RegressionGraphInput input) {
		
		List<GraphData> matchingGraphs = new ArrayList<GraphData>();

		
		for (GraphPoint gp : graph.points) {

			DateTime gpTime = TimeUtil.getDateTime(gp.time);
			Long epochTime = Long.valueOf(gpTime.getMillis());
			
			if (gp.contributors == null) {
				
				for (GraphData graphData : graphsData.values()) {
					graphData.points.put(epochTime, 0l);
				}
				
				continue;
			}
			
			matchingGraphs.clear();

			for (GraphPointContributor gpc : gp.contributors) {

				EventResult event = getEvent(eventData, gpc.id);
				
				if (event == null) {
					continue;
				}
				
				String key = formatLocation(event.error_location);
				
				if (key == null) {
					continue;
				}
				
				GraphData graphData = graphsData.get(key);

				if (graphData == null) {
					graphData = new GraphData(key);
					graphsData.put(key, graphData);
				}
				
				long pointValue;
				
				if ((input.graphType == null) || (input.graphType.equals(GraphType.Percentage))) {
					
					if (gpc.stats.invocations > 0) {
						pointValue = 100 * gpc.stats.hits /  gpc.stats.invocations;
					} else {
						pointValue = 0l;
					}

				} else {	
					if (input.volumeType.equals(VolumeType.invocations)) {
						pointValue = gpc.stats.invocations;
					} else {
						pointValue = gpc.stats.hits;	
					}
				}
				
				graphData.volume += pointValue;
				
				Long existingValue = graphData.points.get(epochTime);
				
				if (existingValue != null) {
					graphData.points.put(epochTime, Long.valueOf(existingValue.longValue() + pointValue));
				} else {
					graphData.points.put(epochTime, Long.valueOf(pointValue));
				}	
				
				matchingGraphs.add(graphData);

			}
			
			for (GraphData graphData : graphsData.values()) {
				
				if (!matchingGraphs.contains(graphData)) {
					graphData.points.put(epochTime, 0l);
				}
			}	
		}
	}
	
	private RegressionGraphInput getGraphInput(GraphInput input) {
		
		RegressionGraphInput result;
		RegressionGraphInput rgInput = (RegressionGraphInput)input;

		if (rgInput.timeFilterVar != null) {
			Gson gson = new Gson();
			result = gson.fromJson(gson.toJson(rgInput), rgInput.getClass());
			Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(rgInput.timeFilterVar);
			result.timeFilter = TimeUtil.getTimeFilter(timespan);
		} else {
			result = rgInput;
		}
		
		return result;
	}
	
	@Override
	protected List<GraphSeries> processGraphSeries(Collection<String> servieIds, 
			String serviceId, String viewName, String viewId, Pair<DateTime, DateTime> timeSpan,
			GraphInput input) {
 		
		RegressionGraphInput graphInput = getGraphInput(input);
		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		
		RegressionOutput regressionOutput = regressionFunction.runRegression(serviceId, graphInput);

		if ((regressionOutput == null) || (regressionOutput.rateRegression == null) 
			||  (regressionOutput.regressionInput == null)) {
			return Collections.emptyList();
		}
		
		boolean includeNew;
		boolean includeRegressions;
		
		if ((graphInput.regressionType == null) || (graphInput.regressionType == RegressionType.Regressions)) {
			includeNew = false;
			includeRegressions = true;
		} else {
			includeNew = true;
			includeRegressions = false;
		}
		
		List<EventData> eventDatas = regressionFunction.processRegression(graphInput, regressionOutput.regressionInput,
			regressionOutput.rateRegression, includeNew, includeRegressions);
		
		EventFilter eventFilter = getEventFilter(serviceId, graphInput, timeSpan);
		
		if (eventFilter == null) {
			return Collections.emptyList();	
		}
		
		List<EventData> filteredEventData = new ArrayList<EventData>(eventDatas.size());
		
		for (EventData eventData : eventDatas) {	 
			
			if (eventFilter.filter(eventData.event)) {
				continue;
			}
			
			filteredEventData.add(eventData);
		}
		
		Map<String, GraphData> graphsData = new HashMap<String, GraphData>();

		if (regressionOutput.baseVolumeGraph != null) {
			appendGraphToMap(graphsData, filteredEventData, regressionOutput.baseVolumeGraph, graphInput);
		}
		
		if (regressionOutput.activeVolumeGraph != null) {
			appendGraphToMap(graphsData, filteredEventData, regressionOutput.activeVolumeGraph, graphInput);
		}
			
		List<GraphSeries> result = getGraphSeries(servieIds, serviceId, graphsData, 
			eventDatas, graphInput, includeRegressions);
				
		return result;
	}
	
	private List<GraphSeries> getGraphSeries(Collection<String> serviceIds, String serviceId,
			Map<String, GraphData> graphsData, Collection<EventData> eventDatas, RegressionGraphInput rgInput,
			boolean regressions) {
	
		List<GraphSeries> result = new ArrayList<GraphSeries>();
					
		int limit = Math.min(eventDatas.size(), rgInput.limit);
			
		for (EventData eventData : eventDatas) {
				
			if (!(eventData instanceof RegressionData)) {
				continue;
			}
			
			RegressionData regressionData = (RegressionData)eventData;
			
			if ((regressionData.regression != null) != regressions) {
				continue;
			}
			
			String key = formatLocation(eventData.event.error_location);
			
			if (key == null) {
				continue;
			}
			
			GraphData graphData = graphsData.get(key);
				
			if (graphData != null) {
				result.add(getGraphSeries(serviceIds, serviceId, graphData, rgInput));
			}
								
			if (result.size() >= limit) {
				break;
			}
		}
		
		return result;
	}
	
	private GraphSeries getGraphSeries(Collection<String> serviceIds,
			String serviceId, GraphData graphData, 
		RegressionGraphInput rgInput) {
		
		String seriesName;
		
		if (rgInput.sevSeriesPostfix != null) {
			seriesName = graphData.key + rgInput.sevSeriesPostfix;
		} else {
			seriesName = graphData.key;
		}
		
		return getGraphSeries(graphData, getServiceValue(seriesName, serviceId, 
			serviceIds), rgInput);	
	}
}
