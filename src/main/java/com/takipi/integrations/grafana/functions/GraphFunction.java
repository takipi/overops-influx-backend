package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

		for (EventResult event : events) {
			result.put(event.id, event);
		}

		return result;
	}

	protected List<GraphSeries> processServiceGraph(String serviceId, String viewId, String viewName,
			BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, boolean multiService, int pointsWanted) {

		GraphInput input = (GraphInput) request;

		Graph graph = ApiViewUtil.getEventsGraph(apiClient, serviceId, viewId, pointsWanted, input.volumeType, 
			timeSpan.getFirst(), timeSpan.getSecond());
		
		Series series = new Series();

		String tagName;
		
		if (multiService) {
			tagName = viewName + SERVICE_SEPERATOR + serviceId;
		} else {
			tagName = viewName;
		}

		SeriesVolume seriesData = processGraphPoints(serviceId, viewId, timeSpan, graph, input);

		series.name = EMPTY_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, tagName });
		series.values = seriesData.values;

		return Collections.singletonList(GraphSeries.of(series, seriesData.volume));

	}

	private SeriesVolume processGraphPoints(String serviceId, String viewId, 
			Pair<DateTime, DateTime> timeSpan, Graph graph, GraphInput request) {

		long volume = 0;
		List<List<Object>> values = new ArrayList<List<Object>>(graph.points.size());

		
		Collection<String> types = request.getTypes();
		Collection<String> introducedBy = request.getIntroducedBy(serviceId);
		
		Map<String, EventResult> eventMap;

		if (request.hasIntroducedBy()) {
			eventMap = getEventMap(serviceId, viewId, timeSpan);
		} else {
			eventMap = null;
		}
		
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
					if ((event != null) && (filterEvent(types, introducedBy, event))) {
						continue;
					}
				}

				if (request.volumeType.equals(VolumeType.invocations)) {
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

		GraphInput request = (GraphInput) functionInput;

		if ((request.volumeType == null)) {
			throw new IllegalArgumentException("volumeType");
		}

		return super.process(functionInput);
	}
}
