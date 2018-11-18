package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.input.GraphLimitInput;
import com.takipi.integrations.grafana.output.Series;

public abstract class LimitGraphFunction extends GraphFunction {

	protected static class GraphData {
		protected Map<Long, Long> points;
		protected long volume;
		protected String key;
		
		protected GraphData(String key) {
			this.key = key;
			this.points = new TreeMap<Long, Long>();
		}
	}
	
	public LimitGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

	protected Collection<GraphData> getLimitedGraphData(Collection<GraphData> graphsData, int limit) {
		
		List<GraphData> sorted = new ArrayList<GraphData>(graphsData);
		
		sorted.sort(new Comparator<GraphData>() {

			@Override
			public int compare(GraphData o1, GraphData o2) {
				return (int)(o2.volume - o1.volume);
			}
		});
		
		return sorted.subList(0, Math.min(graphsData.size(), limit));
	}
	
	protected static GraphSeries getGraphSeries(GraphData graphData, String name) {
		
		GraphSeries result = new GraphSeries();
					
		result.series = new Series();
		
		result.series.name = EMPTY_NAME;
		result.series.columns = Arrays.asList(new String[] { TIME_COLUMN, name });
		result.series.values = new ArrayList<List<Object>>();
		result.volume = graphData.volume;
			
		for (Map.Entry<Long, Long> graphPoint : graphData.points.entrySet()) {				
			result.series.values.add(Arrays.asList(new Object[] { graphPoint.getKey(), graphPoint.getValue() }));
		}
		
		return result;
	}
	
	protected abstract List<GraphSeries> processGraphSeries(String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			Graph graph, GraphInput input);

	@Override
	protected List<GraphSeries> processServiceGraph(String serviceId, String viewId, String viewName,
			BaseGraphInput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds, int pointsWanted) {

		GraphInput graphInput = (GraphInput) input;

		Graph graph = getEventsGraph(apiClient, serviceId, viewId, pointsWanted, graphInput, graphInput.volumeType,
				timeSpan.getFirst(), timeSpan.getSecond());

		if (graph == null) {
			return Collections.emptyList();
		}

		List<GraphSeries> result = processGraphSeries(serviceId, viewId, timeSpan,
			graph, graphInput);
		
		return result;
 	}
	
	@Override
	protected List<Series> processSeries(List<GraphSeries> series, BaseGraphInput input) {
		
		GraphLimitInput routingGraphInput = (GraphLimitInput)input;
		List<Series> output = super.processSeries(series, input);
		
		if (routingGraphInput.limit == 0) {
			return output;
		}
		
		List<Series> result = limitGraphSeries(series, routingGraphInput.limit);
		
		sortByName(result);
		
		return result;
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		if (!(functionInput instanceof GraphLimitInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
	}
}
