package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.EventsFunction.EventData;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionData;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.input.RegressionGraphInput;
import com.takipi.integrations.grafana.input.RegressionGraphInput.GraphType;
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
			
			for (String similiarId : regData.mergedIds) {
				if (similiarId.equals(id)) {
					return eventData.event;
				}
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

				GraphData graphData = graphsData.get(key);

				if (graphData == null) {
					graphData = new GraphData(key);
					graphsData.put(key, graphData);
				}
				
				if (gpc.stats.invocations == 0) {
					continue;
				}
				
				long pointValue;
				
				if ((input.graphType == null) || (input.graphType.equals(GraphType.Percentage))) {
					pointValue = 100 * gpc.stats.hits /  gpc.stats.invocations;

				} else {
					pointValue = gpc.stats.hits;

				}
				
				graphData.volume += pointValue;
				graphData.points.put(epochTime, Long.valueOf(pointValue));
				
				matchingGraphs.add(graphData);

			}
			
			for (GraphData graphData : graphsData.values()) {
				
				if (!matchingGraphs.contains(graphData)) {
					graphData.points.put(epochTime, 0l);
				}
			}	
		}
	}
	
	@Override
	protected List<GraphSeries> processGraphSeries(String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			GraphInput input) {
 		
		RegressionGraphInput rgInput = (RegressionGraphInput)input;
		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		
		RegressionOutput regressionOutput = regressionFunction.runRegression(serviceId, rgInput);

		if ((regressionOutput == null) || (regressionOutput.rateRegression == null) 
			||  (regressionOutput.regressionInput == null)) {
			return Collections.emptyList();
		}
		
		List<EventData> eventDatas = regressionFunction.processRegression(rgInput, regressionOutput.regressionInput,
			regressionOutput.rateRegression, false);
		
		
		EventFilter eventFilter = rgInput.getEventFilter(apiClient, serviceId);
		
		List<EventData> filteredEventData = new ArrayList<EventData>(eventDatas.size());
		
		for (EventData eventData : eventDatas) {	 
			
			if (eventFilter.filter(eventData.event)) {
				continue;
			}
			
			filteredEventData.add(eventData);
		}

		List<EventData> limitEventData = filteredEventData.subList(0, Math.min(filteredEventData.size(), rgInput.limit));
		
		Map<String, GraphData> graphsData = new HashMap<String, GraphData>();

		if (regressionOutput.baseVolumeGraph != null) {
			appendGraphToMap(graphsData, limitEventData, regressionOutput.baseVolumeGraph, rgInput);
		}
		
		if (regressionOutput.activeVolumeGraph != null) {
			appendGraphToMap(graphsData, limitEventData, regressionOutput.activeVolumeGraph, rgInput);
		}
	
		List<GraphSeries> result = new ArrayList<GraphSeries>();
		
		for (GraphData graphData : graphsData.values()) {
			
			String seriesName;
			
			if (rgInput.sevSeriesPostfix != null) {
				seriesName = graphData.key + rgInput.sevSeriesPostfix;
			} else {
				seriesName = graphData.key;
			}
			
			result.add(getGraphSeries(graphData, seriesName));	
		}
				
		return result;
	}
}
