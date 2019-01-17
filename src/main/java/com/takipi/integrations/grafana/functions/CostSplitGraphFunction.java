package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.input.GraphCostLimitInput;
import com.takipi.integrations.grafana.util.TimeUtil;

public class CostSplitGraphFunction extends LimitGraphFunction {
	private static final Logger logger = LoggerFactory.getLogger(CostSplitGraphFunction.class); 

	private class CostGraphData extends GraphData {

		private Double costFactor;
		
		protected CostGraphData(String key) {
			super(key);
			costFactor = .0;
		}

		protected CostGraphData(String key, Double cf) {
			super(key);
			costFactor = cf;
		}
		
		public double getCostFactor() {
			return costFactor;
		}
	}
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new CostSplitGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return GraphCostLimitInput.class;
		}

		@Override
		public String getName() {
			return "costSplitGraph";
		}
	}

	public CostSplitGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected List<GraphSeries> processGraphSeries(Collection<String> serviceIds,
			String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			GraphInput input) {
		GraphCostLimitInput limitInput = (GraphCostLimitInput)input;

		Map<String, EventResult> eventMap = getEventMap(serviceId, input, timeSpan.getFirst(), timeSpan.getSecond(),
				null, 0);

		if (eventMap == null) {
			return Collections.emptyList();
		}

		Graph graph = getEventsGraph(serviceId, viewId, input.pointsWanted, input, input.volumeType,
				timeSpan.getFirst(), timeSpan.getSecond());

		if (graph == null) {
			return Collections.emptyList();		
		}

		EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);

		Map<String, CostGraphData> eventsVolume = new HashMap<>();

		for (GraphPoint gp : graph.points) {

			if (gp.contributors == null) {
				continue;
			}

			for (GraphPointContributor gpc : gp.contributors) {
				EventResult event = eventMap.get(gpc.id);

				if ((event == null) || (eventFilter.filter(event))) {
					continue;
				}


				if (event.error_location == null)
				{
					logger.warn("Event {} does not have error location!", event.id);
					continue;
				}

				Double evCost = limitInput.costData.calculateCost(event.type);

				if (evCost != .0) {
					String key = getTypeAndSimpleClassName(event.type,event.error_location.class_name, event.error_location.method_name);

					CostGraphData graphData = eventsVolume.get(key);

					if (graphData == null) {
						graphData = new CostGraphData(key, evCost);
						eventsVolume.put(key, graphData);
					}

					graphData.volume += gpc.stats.hits;
				}
			}
		}

		Collection<CostGraphData> eventsCostVolume = new ArrayList<>();

		eventsVolume.forEach((evk,evv)->{
			CostGraphData ecvGD = new CostGraphData(evk, evv.getCostFactor());
			ecvGD.volume = (long) (evv.volume 
					// * evv.getCostFactor()
					);
			if (ecvGD.volume != 0L) {
				eventsCostVolume.add(ecvGD);
			}
		});

		Collection<CostGraphData> limitedGraphs = getLimitedPercentageGraphData(eventsCostVolume, limitInput.limit);

		Map<String, GraphData> graphsData = new HashMap<String, GraphData>();

		for (GraphData graphData : limitedGraphs) {
			graphsData.put(graphData.key, graphData);
		}

		List<GraphData> matchingGraphs = new ArrayList<GraphData>();

		for (GraphPoint gp : graph.points) {

			DateTime gpTime = TimeUtil.getDateTime(gp.time);
			Long epochTime = Long.valueOf(gpTime.getMillis());

			if (gp.contributors == null) {

				for (GraphData graphData : limitedGraphs) {
					graphData.points.put(epochTime, 0l);
				}

				continue;
			}

			matchingGraphs.clear();

			for (GraphPointContributor gpc : gp.contributors) {
				EventResult event = eventMap.get(gpc.id);

				if ((event == null) || (eventFilter.filter(event))) {
					continue;
				}

				if (event.error_location == null)
				{
					logger.warn("Event {} does not have error location!", event.id);
					continue;
				}

				long pointValue;

				if (input.volumeType.equals(VolumeType.invocations)) {
					pointValue = gpc.stats.invocations;
				} else {
					pointValue = gpc.stats.hits;
				}

				String key = getTypeAndSimpleClassName(event.type,event.error_location.class_name, event.error_location.method_name);

				GraphData graphData = graphsData.get(key);

				if (graphData == null) {
					continue;
				}

				graphData.volume += pointValue;

				Long newValue;
				Long existingValue = graphData.points.get(epochTime);

				if (existingValue != null) {
					newValue = Long.valueOf(pointValue + existingValue.longValue());
				} else {
					newValue = Long.valueOf(pointValue);
				}

				graphData.points.put(epochTime, newValue);
				matchingGraphs.add(graphData);
			}

			for (GraphData graphData : limitedGraphs) {

				if (!matchingGraphs.contains(graphData)) {
					graphData.points.put(epochTime, 0l);
				}
			}	
		}

		List<GraphSeries> result = new ArrayList<GraphSeries>();

		Long epochTimeNow = Long.valueOf(DateTime.now().getMillis());

		String graphTrendCalc = "AVG";	// This serves as default and uses AVG function
		if (limitInput.graphTrendType != null && !limitInput.graphTrendType.isEmpty()) {
			String gtt = limitInput.graphTrendType.trim().toUpperCase();
			if (gtt.equals("SPOT") || gtt.equals("BALANCE")) {
				graphTrendCalc = gtt;
			}
		}

		//TODO simplify the used cal with ENUM etc.
		for (CostGraphData graphData : limitedGraphs) {

			if (graphData.volume > 0) {
				Double gdCost = graphData.costFactor;
				Long valueAccumulator = 0l;
				Long accumulatorEntries = 0l;
				Long calculatedValue= 0l;
				for (Map.Entry<Long,Long> gdP : graphData.points.entrySet()) {
					if (graphTrendCalc.equals("SPOT")) {
						calculatedValue = (long) (gdP.getValue() * gdCost);
					} else {
						valueAccumulator += gdP.getValue();
						++accumulatorEntries;
						if (gdP.getKey() <= epochTimeNow) {
							if (graphTrendCalc.equals("BALANCE")) {
								calculatedValue = (long) (valueAccumulator * gdCost);						// Total have balance so far
							} else {
								calculatedValue = (long) (valueAccumulator / accumulatorEntries * gdCost);	// AVG from beginning to point
							}
						} else {
							calculatedValue = 0l;
						}
					}
					gdP.setValue(calculatedValue);
				}
				result.add(getGraphSeries(graphData, getServiceValue(graphData.key, serviceId, serviceIds)));
			}
		}
		return result;
	}
}

