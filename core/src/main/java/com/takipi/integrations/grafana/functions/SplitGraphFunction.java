package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
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
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.input.GraphLimitInput;
import com.takipi.integrations.grafana.util.TimeUtil;

public class SplitGraphFunction extends LimitGraphFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new SplitGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return GraphLimitInput.class;
		}

		@Override
		public String getName() {
			return "splitGraph";
		}
	}

	public SplitGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected List<GraphSeries> processGraphSeries(Collection<String> serviceIds,
			String serviceId, String viewName, String viewId, Pair<DateTime, DateTime> timeSpan,
			GraphInput input) {
		GraphLimitInput limitInput = (GraphLimitInput)input;

		Map<String, EventResult> eventMap = getEventMap(serviceId, input, timeSpan.getFirst(), timeSpan.getSecond(),
				null, 0);

		if (eventMap == null) {
			return Collections.emptyList();
		}
		
		Graph graph = getEventsGraph(serviceId, viewId, input.pointsWanted, input, input.volumeType,
				timeSpan.getFirst(), timeSpan.getSecond());
		
		if (graph == null) {
			return Collections.emptyList();		
		}
		
		EventFilter eventFilter = getEventFilter(serviceId, input, timeSpan);
		
		if (eventFilter == null) {
			return Collections.emptyList();		
		}
		
		Map<String, GraphData> eventsVolume = new HashMap<String, GraphData>();
		
		for (GraphPoint gp : graph.points) {

			if (gp.contributors == null) {
				continue;
			}

			for (GraphPointContributor gpc : gp.contributors) {
				EventResult event = eventMap.get(gpc.id);

				if ((event == null) || (eventFilter.filter(event))) {
					continue;
				}
				
				if (event.error_location == null) {
					continue;
				}
				
				String key = getSimpleClassName(event.error_location.class_name);

				GraphData graphData = eventsVolume.get(key);

				if (graphData == null) {
					graphData = new GraphData(key);
					eventsVolume.put(key, graphData);
				}
				
				graphData.volume += gpc.stats.hits;
			}
		}
		
		Collection<GraphData> limitedGraphs = getLimitedGraphData(eventsVolume.values(), limitInput.limit);

		Map<String, GraphData> graphsData = new HashMap<String, GraphData>();
		
		for (GraphData graphData : limitedGraphs) {
			graphsData.put(graphData.key, graphData);
		}
		
		List<GraphData> matchingGraphs = new ArrayList<GraphData>();
		
		for (GraphPoint gp : graph.points) {

			DateTime gpTime = TimeUtil.getDateTime(gp.time);
			Long epochTime = Long.valueOf(gpTime.getMillis());
			
			if (gp.contributors == null) {
				
				for (GraphData graphData : limitedGraphs) {
					graphData.points.put(epochTime, 0l);
				}
				
				continue;
			}
			
			matchingGraphs.clear();
		
			for (GraphPointContributor gpc : gp.contributors) {
				EventResult event = eventMap.get(gpc.id);

				if ((event == null) || (eventFilter.filter(event))) {
					continue;
				}
				
				if (event.error_location == null) {
					continue;
				}
				
				long pointValue;

				if (input.volumeType.equals(VolumeType.invocations)) {
					pointValue = gpc.stats.invocations;
				} else {
					pointValue = gpc.stats.hits;
				}
				
				String key = getSimpleClassName(event.error_location.class_name);
				
				GraphData graphData = graphsData.get(key);

				if (graphData == null) {
					continue;
				}
				
				graphData.volume += pointValue;
				
				Long newValue;
				Long existingValue = graphData.points.get(epochTime);
				
				if (existingValue != null) {
					newValue = Long.valueOf(pointValue + existingValue.longValue());
				} else {
					newValue = Long.valueOf(pointValue);
				}
				
				graphData.points.put(epochTime, newValue);
				matchingGraphs.add(graphData);
			}
			
			for (GraphData graphData : limitedGraphs) {
				
				if (!matchingGraphs.contains(graphData)) {
					graphData.points.put(epochTime, 0l);
				}
			}	
		}
		
		List<GraphSeries> result = new ArrayList<GraphSeries>();
		
		for (GraphData graphData : limitedGraphs) {
			
			if (graphData.volume > 0) {
				result.add(getGraphSeries(graphData, 
					getServiceValue(graphData.key, serviceId, serviceIds), input));
			}
		}
				
		return result;
	}
}
