package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.input.GraphLimitInput;
import com.takipi.integrations.grafana.output.Series;

public abstract class LimitGraphFunction extends GraphFunction {

	public LimitGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	protected void sortGraphData(List<GraphData> graphsData) {
				
		graphsData.sort(new Comparator<GraphData>() {

			@Override
			public int compare(GraphData o1, GraphData o2) {
				return (int)(o2.volume - o1.volume);
			}
		});
	}
	
	protected List<GraphData> getLimitedGraphData(Collection<GraphData> graphsData, int limit) {
		
		List<GraphData> sorted = new ArrayList<GraphData>(graphsData);
		sortGraphData(sorted);
		return sorted.subList(0, Math.min(graphsData.size(), limit));
	}
	
	protected abstract List<GraphSeries> processGraphSeries(Collection<String> serviceIds,
			String serviceId, String viewName, String viewId, Pair<DateTime, DateTime> timeSpan,
			GraphInput input);

	@Override
	protected List<GraphSeries> processServiceGraph(Collection<String> serviceIds, String serviceId, 
		String viewId, String viewName, BaseGraphInput input, 
		Pair<DateTime, DateTime> timeSpan, Object tag) {

		GraphInput graphInput = (GraphInput) input;

		List<GraphSeries> result = processGraphSeries(serviceIds, serviceId, 
				viewName, viewId, timeSpan, graphInput);
		
		return result;
 	}
	
	@Override
	protected List<Series> processSeries(Collection<String> serviceIds,
		List<GraphSeries> series, BaseGraphInput input) {
		
		sortSeriesByName(series);
		
		GraphLimitInput graphLimitInput = (GraphLimitInput)input;
		List<Series> output = super.processSeries(serviceIds, series, input);
		
		if (graphLimitInput.limit == 0) {
			return output;
		}
		
		List<Series> result = limitSeries(series, graphLimitInput.limit);
				
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
