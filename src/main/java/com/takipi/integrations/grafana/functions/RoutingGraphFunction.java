
package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
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
import com.takipi.integrations.grafana.utils.TimeUtils;
import com.takipi.udf.infra.Categories;

public class RoutingGraphFunction extends LimitGraphFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new RoutingGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return GraphLimitInput.class;
		}

		@Override
		public String getName() {
			return "routingGraph";
		}
	}

	public RoutingGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected List<GraphSeries> processGraphSeries(String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			Graph graph, GraphInput input) {
		
		GraphLimitInput limitInput = (GraphLimitInput)input;
		
		Map<String, EventResult> eventMap = getEventMap(serviceId, input, timeSpan.getFirst(), timeSpan.getSecond(), input.volumeType,
					input.pointsWanted);
		
		EventFilter eventFilter = input.getEventFilter(serviceId);

		Set<GraphData> graphsInPoint = new HashSet<GraphData>();
		Map<String, GraphData> graphsData = new HashMap<String, GraphData>();
		
		Long key = null;
		Long lastKey = null;
		
		for (GraphPoint gp : graph.points) {

			if (gp.contributors == null) {
				continue;
			}

			DateTime gpTime = TimeUtils.getDateTime(gp.time);
			
			lastKey = key;
			key = Long.valueOf(gpTime.getMillis());
			
			for (GraphPointContributor gpc : gp.contributors) {

				EventResult event = eventMap.get(gpc.id);

				if ((event == null) || (event.error_origin == null) || (eventFilter.filter(event))) {
					continue;
				}

				Set<String> labels = Categories.defaultCategories()
						.getCategories(event.error_origin.class_name);
				
				if (labels == null) {
					continue;
				}
				
				long pointValue;
				
				if (input.volumeType.equals(VolumeType.invocations)) {
					pointValue = gpc.stats.invocations;
				} else {
					pointValue = gpc.stats.hits;
				}

				for (String label : labels) {
					GraphData graphData = graphsData.get(label);
					
					if (graphData == null) {
						graphData = new GraphData(label);
						
						if (lastKey != null) {
							graphData.points.put(lastKey, Long.valueOf(0l));

						}
						
						graphsData.put(label, graphData);
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
			}
			
			for (GraphData graphData : graphsData.values()) {
				if (!graphsInPoint.contains(graphData)) {
					graphData.points.put(key, Long.valueOf(0l));
				}
			}
			
			graphsInPoint.clear();	
		}
		
		Collection<GraphData> limitedGraphs = getLimitedGraphData(graphsData.values(), limitInput.limit);
		
		List<GraphSeries> result = new ArrayList<GraphSeries>();
		
		for (GraphData graphData : limitedGraphs) {
			result.add(getGraphSeries(graphData, graphData.key));	
		}
				
		return result;

	}
}
