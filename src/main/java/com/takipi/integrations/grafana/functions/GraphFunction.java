package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.TimeUtil;

public class GraphFunction extends BaseGraphFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new GraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return GraphInput.class;
		}

		@Override
		public String getName() {
			return "graph";
		}
	}
	
	protected static class SeriesVolume {
		
		protected List<List<Object>> values;
		protected long volume;
		
		protected static SeriesVolume of(List<List<Object>> values, long volume) {
			SeriesVolume result = new SeriesVolume();
			result.volume = volume;
			result.values = values;
			return result;
		}
	}
	
	public GraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

	protected List<Series> limitGraphSeries(List<GraphSeries> series, int limit) {
		
		List<GraphSeries> sorted = new ArrayList<GraphSeries>(series);
		
		sortSeriesByVolume(sorted);
		
		List<Series> result = new ArrayList<Series>();
		
		for (int i = 0; i < Math.min(limit, sorted.size()); i++) {
			
			GraphSeries graphSeries = sorted.get(i);
			
			if (graphSeries.volume > 0) {
				result.add(graphSeries.series);
			}
		}
		return result;
	}
	
	protected void sortSeriesByVolume(List<GraphSeries> series) {
		series.sort(new Comparator<GraphSeries>() {

			@Override
			public int compare(GraphSeries o1, GraphSeries o2) {
				return (int)(o2.volume - o1.volume);
			}
		});
	}
	
	@Override
	protected List<GraphSeries> processServiceGraph(String serviceId, String viewId, String viewName,
			BaseGraphInput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds, int pointsWanted) {

		GraphInput graphInput = (GraphInput) input;

		Graph graph = getEventsGraph(apiClient, serviceId, viewId, pointsWanted, graphInput, 
			graphInput.volumeType, timeSpan.getFirst(), timeSpan.getSecond());
		
		if (graph == null) {
			return Collections.emptyList();
		}
		
		Series series = new Series();

		String tagName = getSeriesName(input, input.seriesName, viewName, serviceId, serviceIds);
		SeriesVolume seriesData = processGraphPoints(serviceId, viewId, timeSpan, graph, graphInput);

		series.name = EMPTY_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, tagName });
		
		if ((graphInput.condense) && (seriesData.values.size() > pointsWanted)) {
			series.values = condensePoints(seriesData.values, pointsWanted);
		} else {
			series.values = seriesData.values;
		}

		return Collections.singletonList(GraphSeries.of(series, seriesData.volume));

	}
	
	private static long getPointTime(List<List<Object>> points, int index) {
		return ((Long)(points.get(index).get(0))).longValue();
	}
	
	private static long getPointValue(List<List<Object>> points, int index) {
		return ((Long)(points.get(index).get(1))).longValue();
	}
	
	private List<List<Object>> condensePoints(List<List<Object>> points, int pointsWanted) {
		
		double groupSize = (points.size() - 2) / ((double)pointsWanted - 2);
		double currSize = groupSize;

		long[] values = new long[pointsWanted - 2];

		int index = 0;
		
		for (int i = 1; i < points.size() - 1; i++) {
			
			long pointValue = getPointValue(points, i);
			
			if (currSize >= 1) {
				values[index] += pointValue;	
				currSize--;
			} else {
				
				values[index] += pointValue * currSize;
				index++;
				values[index] += pointValue * (1 - currSize);
				currSize = groupSize - (1 - currSize);
			}
		}
		
		List<List<Object>> result = new ArrayList<List<Object>>(pointsWanted);
		
		long start = getPointTime(points, 0);
		long end = getPointTime(points, points.size() - 1);
	
		long timeDelta = (end - start) / (pointsWanted -1);
		
		result.add(points.get(0));
		
		for (int i = 0; i < values.length; i++) {
			
			long time = start + timeDelta * (i + 1);
			long value = (long)values[i] / (long)groupSize;
			result.add(Arrays.asList(new Object[] {Long.valueOf(time), Long.valueOf(value) }));
		}

		result.add(points.get(points.size() - 1));

		return result;
	}

	protected SeriesVolume processGraphPoints(String serviceId, String viewId, 
			Pair<DateTime, DateTime> timeSpan, Graph graph, GraphInput input) {

		long volume = 0;
	
		List<List<Object>> values = new ArrayList<List<Object>>(graph.points.size());
		
		EventFilter eventFilter;
		Map<String, EventResult> eventMap;
		
		if (input.hasEventFilter()) {
			eventMap = getEventMap(serviceId, input, timeSpan.getFirst(), timeSpan.getSecond(),
				input.volumeType, input.pointsWanted);
			eventFilter = input.getEventFilter(apiClient, serviceId);

		} else {
			eventMap= null;
			eventFilter = null;
		}
		
		for (GraphPoint gp : graph.points) {

			if (gp.contributors == null) {
				continue;
			}
			
			long value = 0;
			DateTime gpTime = TimeUtil.getDateTime(gp.time);

			for (GraphPointContributor gpc : gp.contributors) {

				if (eventMap != null) {
					EventResult event = eventMap.get(gpc.id);

					//if the event wasn't found we err on the side of adding its stats.
					if ((event != null) && (eventFilter != null) && (eventFilter.filter(event))) {
						continue;
					}
				}

				if (input.volumeType.equals(VolumeType.invocations)) {
					value += gpc.stats.invocations;
				} else {
					value += gpc.stats.hits;
				}
			}

			volume += value;
			values.add(Arrays.asList(new Object[] { Long.valueOf(gpTime.getMillis()), Long.valueOf(value) }));
		}
		
		return SeriesVolume.of(values,volume);
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {
		if (!(functionInput instanceof GraphInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		GraphInput input = (GraphInput) functionInput;

		if ((input.volumeType == null)) {
			throw new IllegalArgumentException("volumeType");
		}

		return super.process(functionInput);
	}
}
