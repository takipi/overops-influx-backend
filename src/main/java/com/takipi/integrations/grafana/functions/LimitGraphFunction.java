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
	
	protected <T extends GraphData> void sortGraphData(List<T> graphsData) {
				
		graphsData.sort(new Comparator<T>() {

			@Override
			public int compare(T o1, T o2) {
				return (int)(o2.volume - o1.volume);
			}
		});
	}
	
	protected List<GraphData> getLimitedGraphData(Collection<GraphData> graphsData, int limit) {
		
		List<GraphData> sorted = new ArrayList<GraphData>(graphsData);
		sortGraphData(sorted);
		return sorted.subList(0, Math.min(graphsData.size(), limit));
	}

	protected <T extends GraphData> List<T> getLimitedPercentageGraphData(Collection<T> graphsData, int limit) {
		
		List<T> sorted = new ArrayList<T>(graphsData);
		sortGraphData(sorted);
		long totalVol = 0;
		for (GraphData gd: graphsData) {
			totalVol += gd.volume;
		}
		long targetVol = Math.min(100, Math.max(0, limit)) * totalVol / 100;
		long resultVol=0;
		int i = 0;
		while (resultVol < targetVol && i < graphsData.size()) {
			resultVol += sorted.get(i).volume;
			if (resultVol >= targetVol)
				break;
			else
				++i;
		}
		return sorted.subList(0, Math.min(graphsData.size(), ++i));
	}
	
	protected abstract List<GraphSeries> processGraphSeries(String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			GraphInput input);

	@Override
	protected List<GraphSeries> processServiceGraph(String serviceId, String viewId, String viewName,
			BaseGraphInput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds, int pointsWanted) {

		GraphInput graphInput = (GraphInput) input;

		List<GraphSeries> result = processGraphSeries(serviceId, viewId, timeSpan, graphInput);
		
		return result;
 	}
	
	@Override
	protected List<Series> processSeries(List<GraphSeries> series, BaseGraphInput input) {
		
		GraphLimitInput graphLimitInput = (GraphLimitInput)input;
		List<Series> output = super.processSeries(series, input);
		
		if (graphLimitInput.limit == 0) {
			return output;
		}
		
		List<Series> result = limitGraphSeries(series, graphLimitInput.limit);
		
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
