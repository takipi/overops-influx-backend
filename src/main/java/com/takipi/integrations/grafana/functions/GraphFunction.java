package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.metrics.Graph;
import com.takipi.common.api.data.metrics.Graph.GraphPoint;
import com.takipi.common.api.data.metrics.Graph.GraphPointContributor;
import com.takipi.common.api.result.event.EventResult;
import com.takipi.common.api.util.Pair;
import com.takipi.common.api.util.ValidationUtil.VolumeType;
import com.takipi.common.udf.util.ApiViewUtil;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.output.Series;

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

	private Map<String, EventResult> getEventMap(String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan) {
		
		Map<String, EventResult> result = new HashMap<String, EventResult>();
		List<EventResult> events = ApiViewUtil.getEvents(apiClient, serviceId, viewId, timeSpan.getFirst(), timeSpan.getSecond());

		if (events == null) {
			return Collections.emptyMap();
		}
		
		for (EventResult event : events) {
			result.put(event.id, event);
		}

		return result;
	}

	protected List<GraphSeries> processServiceGraph(String serviceId, String viewId, String viewName,
			BaseGraphInput input, Pair<DateTime, DateTime> timeSpan, String[] serviceIds, int pointsWanted) {

		GraphInput graphInput = (GraphInput) input;

		Graph graph = getEventsGraph(apiClient, serviceId, viewId, pointsWanted, graphInput, 
			graphInput.volumeType, timeSpan.getFirst(), timeSpan.getSecond());
		
		if (graph == null) {
			return Collections.emptyList();
		}
		
		Series series = new Series();

		String tagName = getSeriesName(input.seriesName, viewName, serviceId, serviceIds);
		SeriesVolume seriesData = processGraphPoints(serviceId, viewId, timeSpan, graph, graphInput);

		series.name = EMPTY_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, tagName });
		series.values = seriesData.values;

		return Collections.singletonList(GraphSeries.of(series, seriesData.volume));

	}

	private SeriesVolume processGraphPoints(String serviceId, String viewId, 
			Pair<DateTime, DateTime> timeSpan, Graph graph, GraphInput input) {

		long volume = 0;
		List<List<Object>> values = new ArrayList<List<Object>>(graph.points.size());

		
		EventFilter eventFilter = input.getEventFilter(serviceId);

		Map<String, EventResult> eventMap = getEventMap(serviceId, viewId, timeSpan);
		
		for (GraphPoint gp : graph.points) {

			if (gp.contributors == null) {
				continue;
			}
			
			long value = 0;
			DateTime gpTime = ISODateTimeFormat.dateTimeParser().parseDateTime(gp.time);

			for (GraphPointContributor gpc : gp.contributors) {

				if (eventMap != null) {
					EventResult event = eventMap.get(gpc.id);

					//if the event wasn't found we err on the side of adding its stats.
					if ((event != null) && (eventFilter.filter(event))) {
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
			values.add(Arrays.asList(new Object[] { Long.valueOf(gpTime.getMillis()), value }));
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
