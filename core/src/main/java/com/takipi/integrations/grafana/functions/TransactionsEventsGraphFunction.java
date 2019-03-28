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

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.settings.GroupSettings.GroupFilter;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.input.GraphLimitInput;
import com.takipi.integrations.grafana.input.TiersGraphInput;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TransactionsEventsGraphFunction extends LimitGraphFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsEventsGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TiersGraphInput.class;
		}

		@Override
		public String getName() {
			return "transactionsEventsGraph";
		}
	}

	public TransactionsEventsGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	protected List<GraphSeries> processTypesGraphSeries(Collection<String> serviceIds,
			String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			GraphInput input, Collection<String> types) {
		
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
		
		Map<String, EventFilter> eventFilters = new HashMap<String, EventFilter>();
		
		String json = new Gson().toJson(input);
		
		for (String type : types) {
			Gson gson = new Gson();
			GraphLimitInput typeInput = gson.fromJson(json, limitInput.getClass());
			typeInput.types = type;
			EventFilter eventFilter = getEventFilter(serviceId, typeInput, timeSpan);
			
			if (eventFilter != null) {
				eventFilters.put(type, eventFilter);
			}
		}
			
		Set<GraphData> graphsInPoint = new HashSet<GraphData>();
		Map<String, GraphData> graphsData = new HashMap<String, GraphData>();
		
		Long key = null;
		Long lastKey = null;
				
		Set<String> eventTypes = new HashSet<String>();
		
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

				if (event == null) {
					continue;
				}
				
				eventTypes.clear();
				
				for (Map.Entry<String, EventFilter> entry : eventFilters.entrySet()) {
					
					String filterName = entry.getKey();
					EventFilter eventFilter = entry.getValue();
					
					if (!eventFilter.filter(event)) {
						eventTypes.add(filterName);
					}
				}
				
				if (eventTypes.size() == 0) {
					continue;
				}
				
				long pointValue;
				
				if (input.volumeType.equals(VolumeType.invocations)) {
					pointValue = gpc.stats.invocations;
				} else {
					pointValue = gpc.stats.hits;
				}

				for (String type : eventTypes) {
					GraphData graphData = graphsData.get(type);
					
					if (graphData == null) {
						graphData = new GraphData(type);
						
						if (lastKey != null) {
							graphData.points.put(lastKey, Long.valueOf(0l));

						}
						
						graphsData.put(type, graphData);
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
		
		List<GraphSeries> result = new ArrayList<GraphSeries>();
		
		for (GraphData graphData : graphsData.values()) {
			result.add(getGraphSeries(graphData, getServiceValue(graphData.key, 
				serviceId, serviceIds), input));	
		}
		
		sortSeriesByName(result);
				
		return result;
	}

	@Override
	protected List<GraphSeries> processGraphSeries(Collection<String> serviceIds,
			String serviceId, String viewName, String viewId, Pair<DateTime, DateTime> timeSpan,
			GraphInput input) {
		
		GraphLimitInput limitInput = (GraphLimitInput)input;
		
		Collection<String> transactions = input.getTransactions(serviceId);
			
		if (CollectionUtil.safeIsEmpty(transactions)) {
			
			Collection<String> types = input.getTypes(apiClient, serviceId);
			
			if (!CollectionUtil.safeIsEmpty(types)) {
				return processTypesGraphSeries(serviceIds, serviceId, viewId, 
					timeSpan, limitInput, types);

			} else {
				return doProcessServiceGraph(serviceIds, serviceId, viewId,
					limitInput, timeSpan, limitInput.pointsWanted);
			}
		}
		
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

		Map<String, GroupFilter> transactionFilters = new HashMap<String, GroupFilter>();
		
		for (String transaction : transactions) {
			
			GroupFilter transactionFilter = getTransactionsFilter(serviceId, limitInput, timeSpan,
				Collections.singletonList(transaction));
			
			if (transactionFilter != null) {
				transactionFilters.put(transaction, transactionFilter);
			}
		}
		
		Set<GraphData> graphsInPoint = new HashSet<GraphData>();
		Map<String, GraphData> graphsData = new HashMap<String, GraphData>();
		
		Long key = null;
		Long lastKey = null;
				
		Set<String> eventTransactions = new HashSet<String>();
		
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

				if ((event == null) || (event.entry_point == null) || (eventFilter.filter(event))) {
					continue;
				}
				
				eventTransactions.clear();
				
				for (Map.Entry<String, GroupFilter> entry : transactionFilters.entrySet()) {
					
					String filterName = entry.getKey();
					GroupFilter groupFilter = entry.getValue();
					
					if (!filterTransaction(groupFilter, input.searchText, 
							event.entry_point.class_name, event.entry_point.method_name)) {
						
						if (GrafanaFunction.TOP_TRANSACTION_FILTERS.contains(filterName)) {
							
							String keyName = formatLocation(event.entry_point.class_name, 
									event.entry_point.method_name);
							
							String className = getSimpleClassName(event.entry_point.class_name);
							
							for (String filterTransaction : groupFilter.values) {
																
								if (filterTransaction.equals(className)) {
									eventTransactions.add(keyName);
								}
							}
						} else {
							eventTransactions.add(filterName);
						}
					}
				}
				
				if (eventTransactions.size() == 0) {
					continue;
				}
				
				long pointValue;
				
				if (input.volumeType.equals(VolumeType.invocations)) {
					pointValue = gpc.stats.invocations;
				} else {
					pointValue = gpc.stats.hits;
				}

				for (String transaction : eventTransactions) {
					GraphData graphData = graphsData.get(transaction);
					
					if (graphData == null) {
						graphData = new GraphData(transaction);
						
						if (lastKey != null) {
							graphData.points.put(lastKey, Long.valueOf(0l));

						}
						
						graphsData.put(transaction, graphData);
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
		
		List<GraphSeries> result = new ArrayList<GraphSeries>();
		
		for (GraphData graphData : graphsData.values()) {
			result.add(getGraphSeries(graphData, getServiceValue(graphData.key, 
				serviceId, serviceIds), input));	
		}
		
		sortSeriesByName(result);
				
		return result;
	}
}
