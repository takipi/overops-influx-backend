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
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.infra.Categories.CategoryType;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.input.GraphLimitInput;
import com.takipi.integrations.grafana.input.TiersGraphInput;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TiersGraphFunction extends LimitGraphFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TiersGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TiersGraphInput.class;
		}

		@Override
		public String getName() {
			return "routingGraph";
		}
	}

	public TiersGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected List<GraphSeries> processGraphSeries(Collection<String> serviceIds,
			String serviceId, String viewName, String viewId, Pair<DateTime, DateTime> timeSpan,
			GraphInput input) {
		
		GraphLimitInput limitInput = (GraphLimitInput)input;
		
		Map<String, EventResult> eventMap = getEventMap(serviceId, input,
			timeSpan.getFirst(), timeSpan.getSecond(), input.volumeType);
		
		if (eventMap == null) {
			return Collections.emptyList();
		}
		
		Graph graph = getEventsGraph(serviceId, viewId, input, input.volumeType,
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
				
		Categories categories = getSettings(serviceId).getCategories();		
		
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

				Set<String> originLabels = categories.getCategories(
					event.error_origin.class_name, CategoryType.infra);
				
				Set<String> locationLabels = categories.getCategories(
					event.error_location.class_name, CategoryType.infra);

				if ((originLabels == null) && (locationLabels == null)) {
					continue;
				}
				
				Set<String> labels = new HashSet<String>();
				
				if (originLabels != null)  {
					labels.addAll(originLabels);
				}
				
				if (locationLabels != null) {
					labels.addAll(locationLabels);
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
		
		Collection<String> tiers = getSettings(serviceId).getTierNames();
		
		List<GraphData> limitedGraphs = null;
		
		if (tiers != null) {
			limitedGraphs = getKeysTiersGraphData(graphsData, tiers);
			
			if (limitedGraphs.size() < limitInput.limit) {
				
				Collection<GraphData> additionalGraphs = getLimitedGraphData(graphsData.values(),
					limitInput.limit);
				
				for (GraphData graphData : additionalGraphs) {				
					
					if (!limitedGraphs.contains(graphData)) {
						limitedGraphs.add(graphData);
					}
					
					if (limitedGraphs.size() >=  limitInput.limit) {
						break;
					}
				}
			}
		}  else {
			limitedGraphs = getLimitedGraphData(graphsData.values(), limitInput.limit);
		}
		
		List<GraphSeries> result = new ArrayList<GraphSeries>();
		
		for (GraphData graphData : limitedGraphs) {
			result.add(getGraphSeries(graphData, getServiceValue(graphData.key, 
				serviceId, serviceIds), input));	
		}
				
		return result;
	}
	
	private List<GraphData> getKeysTiersGraphData(Map<String, GraphData> graphsData, Collection<String> tiers) {
		
		List<GraphData> result = new ArrayList<GraphData>();
		
		for (String keyTier : tiers) {
			GraphData graphData = graphsData.get(keyTier);
			
			if (graphData != null) {
				result.add(graphData);
			}
		} 
		
		return result;
	}
}
