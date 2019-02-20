package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.input.RegressionSettings;
import com.takipi.integrations.grafana.util.TimeUtil;

public class CriticalExceptionsGraph extends LimitGraphFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new CriticalExceptionsGraph(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return GraphLimitInput.class;
		}

		@Override
		public String getName() {
			return "criticalExceptionsGraph";
		}
	}

	public CriticalExceptionsGraph(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected List<GraphSeries> processGraphSeries(Collection<String> serviceIds,
			String serviceId, String viewName, String viewId, Pair<DateTime, DateTime> timeSpan,
			GraphInput input) {
		
		GraphLimitInput limitInput = (GraphLimitInput)input;
		
		Map<String, EventResult> eventMap = getEventMap(serviceId, input, timeSpan.getFirst(), timeSpan.getSecond(), input.volumeType,
					input.pointsWanted);
		
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
		
		Set<GraphData> graphsInPoint = new HashSet<GraphData>();
		Map<String, GraphData> graphsData = new HashMap<String, GraphData>();
		
		Long key = null;
		Long lastKey = null;
		
		RegressionSettings regressionSettings = GrafanaSettings.getData(apiClient, serviceId).regression;
		
		if (regressionSettings == null) {
			return Collections.emptyList();
		}
		
		Collection<String> criticalExceptionTypes = regressionSettings.getCriticalExceptionTypes();
		
		for (GraphPoint gp : graph.points) {

			DateTime gpTime = TimeUtil.getDateTime(gp.time);
		
			lastKey = key;
			key = Long.valueOf(gpTime.getMillis());

			if (gp.contributors == null) {
				
				for (GraphData graphData : graphsData.values()) {
					graphData.points.put(key, Long.valueOf(0l));
				}
				
				continue;
			}
			
			for (GraphPointContributor gpc : gp.contributors) {

				EventResult event = eventMap.get(gpc.id);

				if ((event == null) || (event.error_location == null) || (event.error_origin == null) || (eventFilter.filter(event))) {
					continue;
				}
				
				if (!criticalExceptionTypes.contains(event.name)) {
					continue;
				}
						
				long pointValue;
				
				if (input.volumeType.equals(VolumeType.invocations)) {
					pointValue = gpc.stats.invocations;
				} else {
					pointValue = gpc.stats.hits;
				}

				GraphData graphData = graphsData.get(event.name);
					
				if (graphData == null) {
					graphData = new GraphData(event.name);
						
					if (lastKey != null) {
						graphData.points.put(lastKey, Long.valueOf(0l));
					}
						
					graphsData.put(event.name, graphData);
				}
								
				Long currValue = graphData.points.get(key);
				Long newValue;
					
				if (currValue == null) {
					newValue = Long.valueOf(pointValue);
				} else {
					newValue = Long.valueOf(currValue.longValue() + pointValue);
				}
					
				graphData.points.put(key, newValue);
				graphData.volume += newValue;
					
				graphsInPoint.add(graphData);
			}
			
			for (GraphData graphData : graphsData.values()) {
				if (!graphsInPoint.contains(graphData)) {
					graphData.points.put(key, Long.valueOf(0l));
				}
			}
			
			graphsInPoint.clear();	
		}
				
		List<GraphSeries> result = new ArrayList<GraphSeries>();
		
		if (limitInput.limit == 0) {
		
			for (GraphData graphData : graphsData.values()) {
				result.add(getGraphSeries(graphData, getServiceValue(graphData.key, 
					serviceId, serviceIds), input));	
			}
		} else {
			
			int size = Math.min(limitInput.limit, criticalExceptionTypes.size());
			
			for (String criticalExceptionType : criticalExceptionTypes) {
				
				GraphData graphData = graphsData.get(criticalExceptionType);
				
				if (graphData == null) {
					continue;
				}
				
				result.add(getGraphSeries(graphData, getServiceValue(graphData.key, 
						serviceId, serviceIds), input));	
				
				if (result.size() >= size) {
					break;
				}
			}
		}
				
		return result;
	}
}
