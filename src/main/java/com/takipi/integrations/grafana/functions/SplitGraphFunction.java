package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.input.GraphLimitInput;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class SplitGraphFunction extends LimitGraphFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new SplitGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return GraphLimitInput.class;
		}

		@Override
		public String getName() {
			return "splitGraph";
		}
	}

	public SplitGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected List<GraphSeries> processGraphSeries(String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			Graph graph, GraphInput input) {

		GraphLimitInput limitInput = (GraphLimitInput)input;

		Map<String, EventResult> eventMap = getEventMap(serviceId, input, timeSpan.getFirst(), timeSpan.getSecond(),
				input.volumeType, input.pointsWanted);

		EventFilter eventFilter = input.getEventFilter(serviceId);

		Map<String, GraphData> graphsData = new HashMap<String, GraphData>();

		for (GraphPoint gp : graph.points) {

			if (gp.contributors == null) {
				continue;
			}

			DateTime gpTime = TimeUtils.getDateTime(gp.time);
			Long epochTime = Long.valueOf(gpTime.getMillis());

			for (GraphPointContributor gpc : gp.contributors) {

				EventResult event = eventMap.get(gpc.id);

				if ((event == null) || (eventFilter.filter(event))) {
					continue;
				}

				long pointValue;

				if (input.volumeType.equals(VolumeType.invocations)) {
					pointValue = gpc.stats.invocations;
				} else {
					pointValue = gpc.stats.hits;
				}

				GraphData graphData = graphsData.get(event.id);

				if (graphData == null) {
					graphData = new GraphData(event.id);
					graphsData.put(event.id, graphData);
				}
				
				graphData.volume += pointValue;
				graphData.points.put(epochTime, Long.valueOf(pointValue));
			}
		}

		Collection<GraphData> limitedGraphs = getLimitedGraphData(graphsData.values(), limitInput.limit);
		
		List<GraphSeries> result = new ArrayList<GraphSeries>();
		
		for (GraphData graphData : limitedGraphs) {
			EventResult event = eventMap.get(graphData.key);
			String location =  formatLocation(event.error_location);
			result.add(getGraphSeries(graphData, location));	
		}
				
		return result;
	}
}
