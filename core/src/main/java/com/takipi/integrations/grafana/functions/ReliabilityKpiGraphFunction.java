package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.performance.PerformanceUtil;
import com.takipi.api.client.util.performance.calc.PerformanceCalculator;
import com.takipi.api.client.util.performance.calc.PerformanceScore;
import com.takipi.api.client.util.performance.transaction.GraphPerformanceCalculator;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.api.client.util.transaction.TransactionUtil;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.EventsFunction.EventData;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionData;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.ReliabilityKpiGraphInput;
import com.takipi.integrations.grafana.input.ReliabilityKpiGraphInput.ReliabilityKpi;
import com.takipi.integrations.grafana.input.ReliabilityReportInput.ReportMode;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings;
import com.takipi.integrations.grafana.settings.input.RegressionReportSettings;
import com.takipi.integrations.grafana.settings.input.SlowdownSettings;
import com.takipi.integrations.grafana.util.TimeUtil;

public class ReliabilityKpiGraphFunction extends BaseGraphFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ReliabilityKpiGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return ReliabilityKpiGraphInput.class;
		}

		@Override
		public String getName() {
			return "ReliabilityKpiGraph";
		}
	}
	
	protected class GraphResultData {
		protected Graph graph;
		protected Map<String, EventResult> eventListMap;
	}
	
	protected abstract class BaseGraphAsyncTask extends BaseAsyncTask {
		
		protected String serviceId;
		protected String viewId;
		protected ReliabilityKpiGraphInput input;
		protected Pair<DateTime, DateTime> timespan;
		
		protected BaseGraphAsyncTask(String serviceId, String viewId, 
			ReliabilityKpiGraphInput input, Pair<DateTime, DateTime> timespan) {
			
			this.serviceId = serviceId;
			this.viewId = viewId;
			this.input = input;
			this.timespan = timespan;
		}
	}
	
	protected class GraphDataTask extends BaseGraphAsyncTask implements Callable<Object>{

		protected GraphDataTask(String serviceId, String viewId, 
			ReliabilityKpiGraphInput input, Pair<DateTime, DateTime> timespan) {
			super(serviceId, viewId, input, timespan);
		}
		
		@Override
		public Object call() throws Exception {
			
			GraphResultData result = new GraphResultData();

			result.graph = getEventsGraph(serviceId, viewId, input.pointsWanted, input, input.volumeType, 
					timespan.getFirst(), timespan.getSecond());
			
			result.eventListMap = getEventMap(serviceId, input,
					timespan.getFirst(), timespan.getSecond(), null, input.pointsWanted);
			
			return result;
		}
	}
	
	protected class TransactionGraphTask extends BaseGraphAsyncTask implements Callable<Object>{
		
		protected TransactionGraphTask(String serviceId, String viewId, 
				ReliabilityKpiGraphInput input, Pair<DateTime, DateTime> timespan) {
			
			super(serviceId, viewId, input, timespan);
		}
		
		@Override
		public Object call() throws Exception {
			
			Collection<TransactionGraph> graphs = getTransactionGraphs(input, serviceId, viewId, timespan, 
					input.getSearchText(), input.transactionPointsWanted, 0, 0);
			
			return graphs;
		}

	}
	
	protected class KpiGraphAsyncTask extends GraphAsyncTask {
		
		protected String app;
		protected boolean isKey;
		protected Collection<Pair<DateTime, DateTime>> periods;
		
		protected KpiGraphAsyncTask(String serviceId, String viewId, String viewName,
				BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, 
				Collection<String> serviceIds, int pointsWanted,
				String app, boolean isKey, Collection<Pair<DateTime, DateTime>> periods) {

			super(serviceId, viewId, viewName, request, timeSpan, serviceIds, pointsWanted);
			this.app = app;
			this.isKey = isKey;
			this.periods = periods;
		}

		@Override
		public Object call() {
			
			beforeCall();
			
			try {
				
				List<GraphSeries> graphSeries = processServiceGraph(serviceIds, serviceId, viewId, 
					input, timeSpan, periods, app, isKey);
				
				return new AsyncResult(graphSeries);
			}
			finally {
				afterCall();
			}
		
		}
		
		@Override
		public String toString() {
			return String.join(" ", "KPI", serviceId, viewId, app, 
				timeSpan.toString(), String.valueOf(pointsWanted));
		}
	}
	
	protected abstract class KpiInterval {
		protected abstract Object getValue(ReliabilityKpi kpi);
	}
	
	protected class RegressionInterval extends KpiInterval {
		
		protected int newErrors;
		protected int severeNewErrors;
		protected int regressions;
		protected int severeRegressions;
		protected RegressionOutput output;
		
		@Override
		protected Object getValue(ReliabilityKpi kpi) {
			
			switch (kpi) {
				
				case NewErrors: 
					return newErrors + severeNewErrors;
				
				case SevereNewErrors: 
					return severeNewErrors;
				
				case IncreasingErrors: 
					return regressions + severeRegressions;
				
				case SevereIncreasingErrors: 
					return severeRegressions;
				
				default: throw new IllegalStateException(kpi.toString());
			}
		}
	}
	
	protected class SlowdownInterval extends KpiInterval {
		
		protected int slowdowns;
		protected int severeSlowdowns;
		
		@Override
		protected Object getValue(ReliabilityKpi kpi) {
			
			switch (kpi) {
				
				case Slowdowns: 
					return slowdowns + severeSlowdowns;
					
				case SevereSlowdowns: 
					return severeSlowdowns;
					
				default: throw new IllegalStateException(kpi.toString());
			}
		}
	}
	
	protected class VolumeInterval extends KpiInterval  {
		
		protected long volume;
		protected long invocations;
		protected Set<String> eventIds;
		
		@Override
		protected Object getValue(ReliabilityKpi kpi) {
			
			switch (kpi) {
				
				case ErrorVolume:
					return volume;
				
				case ErrorRate: 
					
					if (invocations == 0) {
						return 0;
					}
					
					return (double)volume / (double)invocations;
				
				case ErrorCount: 
					return eventIds.size();
				
				default: throw new IllegalStateException(kpi.toString());
			}
		}
		
		protected VolumeInterval() {
			this.eventIds = new HashSet<String>();
		}
	}
	
	protected class ScoreInterval extends KpiInterval {
		
		protected RegressionInterval regressionInterval;
		protected SlowdownInterval slowdownInterval;
		protected double score;
		
		@Override
		protected Object getValue(ReliabilityKpi kpi) {
			return score;
		}
	}

	public ReliabilityKpiGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	private Pair<Collection<TransactionGraph>, Collection<TransactionGraph>> getSlowdownGraphs(Collection<TransactionGraph> graphs, 
			Pair<DateTime, DateTime> period, int baselineWindow) {
			
		List<TransactionGraph> activeGraphs = new ArrayList<TransactionGraph>();
		List<TransactionGraph> baselineGraphs = new ArrayList<TransactionGraph>();

		DateTime baselineEnd = period.getFirst();
		DateTime baselineStart = baselineEnd.minusMinutes(baselineWindow);
		
		for (TransactionGraph graph : graphs) {
				
			TransactionGraph activeGraph = new TransactionGraph();
				
			activeGraph.class_name = graph.class_name;
			activeGraph.method_desc = graph.method_desc;
			activeGraph.method_name = graph.method_name;
			activeGraph.name = graph.name;
			activeGraph.points = new ArrayList<com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint>();
				
			activeGraphs.add(activeGraph);
				
			TransactionGraph baselineGraph = new TransactionGraph();

			baselineGraph.class_name = graph.class_name;
			baselineGraph.method_desc = graph.method_desc;
			baselineGraph.method_name = graph.method_name;
			baselineGraph.name = graph.name;
			baselineGraph.points = new ArrayList<com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint>();
				
			baselineGraphs.add(baselineGraph);

			for (com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint gp : graph.points) {
					
				DateTime gpTime = TimeUtil.getDateTime(gp.time);
					
				if ((gpTime.isAfter(period.getFirst())) 
				&& (gpTime.isBefore(period.getSecond()))) {
					activeGraph.points.add(gp);
					continue;
				}
					
				if ((gpTime.isAfter(baselineStart)) 
				&& (gpTime.isBefore(baselineEnd))) {
					baselineGraph.points.add(gp);
				}	
			}					
		}
		
		return Pair.of(baselineGraphs, activeGraphs);
	}
	
	private Pair<Graph, Graph> getRegressionGraphs(Graph graph, Pair<DateTime, DateTime> period,
		int baselineWindow) {
		
		Graph activeGraph = new Graph();
		Graph baselineGraph = new Graph();
		
		activeGraph.id = graph.id;
		activeGraph.type = graph.type;
		activeGraph.points = new ArrayList<GraphPoint>();
		
		baselineGraph.id = graph.id;
		baselineGraph.type = graph.type;
		baselineGraph.points = new ArrayList<GraphPoint>();

		DateTime baselineEnd = period.getFirst();
		DateTime baselineStart = baselineEnd.minusMinutes(baselineWindow);
		
		for (GraphPoint gp : graph.points) {
			
			DateTime gpTime = TimeUtil.getDateTime(gp.time);
			
			if ((gpTime.isAfter(period.getFirst())) 
			&& (gpTime.isBefore(period.getSecond()))) {
				activeGraph.points.add(gp);
				continue;
			}
			
			if ((gpTime.isAfter(baselineStart)) 
			&& (gpTime.isBefore(baselineEnd))) {
				baselineGraph.points.add(gp);
			}	
		}
		
		return Pair.of(baselineGraph, activeGraph);
	}
		
	private NavigableMap<DateTime, KpiInterval> processSlowdowns(String serviceId,
			String viewId, ReliabilityKpiGraphInput input, 
			Collection<Pair<DateTime, DateTime>> periods, Collection<TransactionGraph> graphs) {
		
		if (graphs == null) {
			return Collections.emptyNavigableMap();
		}

		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		
		SlowdownSettings slowdownSettings = GrafanaSettings.getData(apiClient, serviceId).slowdown;
		
		if (slowdownSettings == null) {
			throw new IllegalStateException("Missing slowdown settings for " + serviceId);
		}
		
		NavigableMap<DateTime, KpiInterval> result = new TreeMap<DateTime, KpiInterval>();
		
		for (Pair<DateTime, DateTime> period : periods) {
			
			Pair<RegressionInput, RegressionWindow> regPair = regressionFunction.getRegressionInput(serviceId, viewId,
					input, period, true);
			
			RegressionInput regressionInput = regPair.getFirst();
			
			Pair<Collection<TransactionGraph>, Collection<TransactionGraph>> graphPairs = getSlowdownGraphs(graphs,
				period, regressionInput.baselineTimespan);
			
			PerformanceCalculator<TransactionGraph> calc = GraphPerformanceCalculator.of(
					slowdownSettings.active_invocations_threshold, slowdownSettings.baseline_invocations_threshold,
					slowdownSettings.min_delta_threshold,
					slowdownSettings.over_avg_slowing_percentage, slowdownSettings.over_avg_critical_percentage,
					slowdownSettings.std_dev_factor);
						
			Map<String, TransactionGraph> baselineGraphsMap = TransactionUtil.getTransactionGraphsMap(graphPairs.getFirst());	
			Map<String, TransactionGraph> activeGraphsMap = TransactionUtil.getTransactionGraphsMap(graphPairs.getSecond());

			Map<TransactionGraph, PerformanceScore> performanceScores = PerformanceUtil.getPerformanceStates(
					activeGraphsMap, baselineGraphsMap, calc);
			
			SlowdownInterval slowdownInterval = new SlowdownInterval();
			
			for (PerformanceScore performanceScore : performanceScores.values()) {
				
				switch (performanceScore.state) {
					
					case CRITICAL:
						slowdownInterval.severeSlowdowns++;
						break;
					
					case SLOWING:
						slowdownInterval.slowdowns++;
						break;
						
					default: continue;
				}	
			}
			
			result.put(period.getFirst(), slowdownInterval);				
		}
		
		return result;
	}
	
	private Map<DateTime, KpiInterval> processVolumesMap(String serviceId,
			String viewId, ReliabilityKpiGraphInput input, 
			Pair<DateTime, DateTime> timespan,
			Collection<Pair<DateTime, DateTime>> periods) {
		
		Graph graph = getEventsGraph(serviceId, viewId, input.pointsWanted, input, input.volumeType, 
				timespan.getFirst(), timespan.getSecond());
		
		if (graph == null) {
			return Collections.emptyMap();
		}
		
		EventFilter eventFilter = getEventFilter(serviceId, input, timespan);

		if (eventFilter == null) {
			return Collections.emptyMap();
		}
		
		Map<String, EventResult> eventMap = getEventMap(serviceId, input,
				timespan.getFirst(), timespan.getSecond(), null, input.pointsWanted);
		
		if (eventMap == null) {
			return Collections.emptyMap();
		}
		
		Map<DateTime, KpiInterval> result = new TreeMap<DateTime, KpiInterval>();
		
		for (GraphPoint gp : graph.points) {
			
			DateTime gpTime = TimeUtil.getDateTime(gp.time);

			for (Pair<DateTime, DateTime> period : periods) {
			
				if ((gpTime.isAfter(period.getFirst())) 
				&& (gpTime.isBefore(period.getSecond()))) {
					
					VolumeInterval volumeInterval = (VolumeInterval)(result.get(period.getFirst()));
					
					if (volumeInterval == null) {
						volumeInterval = new VolumeInterval();
						result.put(period.getFirst(), volumeInterval);
					}
					
					if (CollectionUtil.safeIsEmpty(gp.contributors)) {
						continue;
					}
					
					for (GraphPointContributor gpc : gp.contributors) {
												
						EventResult event = eventMap.get(gpc.id);

						if ((event == null) || (eventFilter.filter(event))) {
							continue;
						}

						volumeInterval.volume += gpc.stats.hits;
						volumeInterval.invocations += gpc.stats.invocations;
						
						volumeInterval.eventIds.add(gpc.id);
					}			
				}
			}
		}
	
		return result;
	}
	
	private void resetStats(Collection<EventResult> events) {
		
		for (EventResult event : events) {
			event.stats.hits = 0;
			event.stats.invocations = 0;
		}
	}
	
	private KpiInterval aggregateRegressions(Map<DateTime, KpiInterval> intervals) {
		
		RegressionInterval result = new RegressionInterval();
		
		for (KpiInterval kpiInterval : intervals.values()) {
			
			RegressionInterval regressionInterval = (RegressionInterval)kpiInterval;
			
			result.newErrors += regressionInterval.newErrors;
			result.severeNewErrors += regressionInterval.severeNewErrors;
			result.regressions += regressionInterval.regressions;
			result.severeRegressions += regressionInterval.severeRegressions;
		}
		
		return result;
	}
	
	private KpiInterval aggregateSlowdowns(Map<DateTime, KpiInterval> intervals) {
		
		SlowdownInterval result = new SlowdownInterval();
		
		for (KpiInterval kpiInterval : intervals.values()) {
			
			SlowdownInterval slowdownInterval = (SlowdownInterval)kpiInterval;
			
			result.slowdowns += slowdownInterval.slowdowns;
			result.severeSlowdowns += slowdownInterval.severeSlowdowns;
		}
		
		return result;
	}
	
	private KpiInterval aggregateScores(Map<DateTime, KpiInterval> intervals) {
		
		ScoreInterval result = new ScoreInterval();
		
		for (KpiInterval kpiInterval : intervals.values()) {
			
			ScoreInterval scoreInterval = (ScoreInterval)kpiInterval;
			
			result.score += scoreInterval.score;
		}
		
		if (intervals.size() > 0) {
			result.score /= intervals.size();
		}
		
		return result;
	}
	
	private KpiInterval aggregateVolumes(Map<DateTime, KpiInterval> intervals) {
		
		VolumeInterval result = new VolumeInterval();
		
		for (KpiInterval kpiInterval : intervals.values()) {
			
			VolumeInterval volumeInterval = (VolumeInterval)kpiInterval;
			
			result.volume += volumeInterval.volume;
			result.invocations += volumeInterval.invocations; 
			result.eventIds.addAll(volumeInterval.eventIds);
		}
				
		return result;	
	}
	
	private NavigableMap<DateTime, KpiInterval> processRegressions(String serviceId,
			String viewId, ReliabilityKpiGraphInput input, ReliabilityKpi kpi,
			Pair<DateTime, DateTime> timespan,
			Collection<Pair<DateTime, DateTime>> periods, GraphResultData graphData) {
		
		if (graphData == null) {
			return Collections.emptyNavigableMap();
		}
		
		if (graphData.eventListMap == null) {
			return Collections.emptyNavigableMap();
		}
				
		if (graphData.graph == null) {
			return Collections.emptyNavigableMap();
		}
		
		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		
		boolean newOnly = (kpi == ReliabilityKpi.NewErrors) || (kpi == ReliabilityKpi.SevereNewErrors);
		
		NavigableMap<DateTime, KpiInterval> result = new TreeMap<DateTime, KpiInterval>();

		for (Pair<DateTime, DateTime> period : periods) {
			
			Pair<RegressionInput, RegressionWindow> regPair = regressionFunction.getRegressionInput(serviceId, viewId,
				input, period, newOnly);
			
			RegressionInput regressionInput = regPair.getFirst();
			RegressionWindow regressionWindow = regPair.getSecond();
			
			Pair<Graph, Graph> graphPair = getRegressionGraphs(graphData.graph, period, regressionInput.baselineTimespan);
			
			Graph baselineGraph = graphPair.getFirst();
			Graph activeGraph = graphPair.getSecond();
			
			resetStats(graphData.eventListMap.values());
			
			Pair<Collection<EventResult>, Long> eventsPair = applyGraphToEvents(serviceId, 
					graphData.eventListMap, timespan, activeGraph, input);
			
			Collection<EventResult> events = eventsPair.getFirst();
			long volume = eventsPair.getSecond().longValue();
			
			regressionInput.events = events;
			regressionInput.baselineGraph = baselineGraph;
			
			RegressionOutput regressionOutput = regressionFunction.executeRegression(input,regressionInput, 
				regressionWindow, graphData.eventListMap, volume, baselineGraph, activeGraph);
			
			RegressionInterval regressionInterval = new RegressionInterval();
			
			regressionInterval.output = regressionOutput;
						
			for (EventData eventData : regressionOutput.eventDatas) {
				
				RegressionData regData = (RegressionData)eventData;
				
				if (regData.event.stats.hits == 0) {
					continue;
				}
				
				switch (regData.type) {
					
					case NewIssues:
						
						DateTime firstSeen = TimeUtil.getDateTime(regData.event.first_seen);

						if ((firstSeen.isAfter(period.getFirst())) 
						&& (firstSeen.isBefore(period.getSecond()))) {
							regressionInterval.newErrors++;
						}	
						
						break;
						
					case SevereNewIssues: 
						
						firstSeen = TimeUtil.getDateTime(regData.event.first_seen);

						if ((firstSeen.isAfter(period.getFirst())) 
						&& (firstSeen.isBefore(period.getSecond()))) {
							regressionInterval.severeNewErrors++;
						}		
						
						break;
					
					case Regressions: 
						
						regressionInterval.regressions++;
						break;
						
					case SevereRegressions: 
						
						regressionInterval.severeRegressions++;
						break;
											
					default:
						throw new IllegalStateException(String.valueOf(kpi));
				}
			}
			
			result.put(period.getFirst(), regressionInterval);
		}
		
		return result;
	}
	
	private Collection<Pair<DateTime, DateTime>> getIntervalPeriods(Pair<DateTime, DateTime> timespan,
		int reportInterval) {
		
		List<Pair<DateTime, DateTime>> result = new ArrayList<Pair<DateTime, DateTime>>();
		
		DateTime periodFrom = timespan.getSecond();
		DateTime periodTo = timespan.getSecond();
		
		while (periodFrom.isAfter(timespan.getFirst())) {
			
			periodFrom = periodFrom.minusMinutes(reportInterval);
			
			if (periodFrom.isAfter(timespan.getFirst())) {
				result.add(Pair.of(periodFrom, periodTo));
			} else {
				result.add(Pair.of(timespan.getFirst(), periodTo));
			}
			
			periodTo = periodFrom;
		}
		
		return result;
	}
	
	private Pair<Map<DateTime, KpiInterval>, KpiInterval> processRegressions(
		String serviceId, String viewId, ReliabilityKpiGraphInput input, 
		Pair<DateTime, DateTime> timespan, Collection<Pair<DateTime, DateTime>> periods,
		ReliabilityKpi kpi) {
		
		GraphDataTask graphTask = new GraphDataTask(serviceId, viewId, input, timespan);
		
		GraphResultData graphResult;
		
		try {
			graphResult = (GraphResultData)(graphTask.call());
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		
		Map<DateTime, KpiInterval> intervals = processRegressions(serviceId, 
			viewId, input, kpi, timespan, periods, graphResult);
		
		KpiInterval aggregate;
		
		if (input.aggregate) {
			aggregate = aggregateRegressions(intervals);
		} else {
			aggregate = null;	
		}
		
		return Pair.of(intervals, aggregate);
	}
	
	@SuppressWarnings("unchecked")
	private Pair<Map<DateTime, KpiInterval>, KpiInterval> processScores(
			String serviceId, String viewId, ReliabilityKpiGraphInput input, 
			Pair<DateTime, DateTime> timespan, 
			Collection<Pair<DateTime, DateTime>> periods, 
			boolean isKey, ReliabilityKpi kpi) {
	
		TransactionGraphTask transactionGraphTask = new TransactionGraphTask(serviceId, viewId, input, timespan);
		GraphDataTask graphTask = new GraphDataTask(serviceId, viewId, input, timespan);

		List<Object> taskResults = executeTasks(Arrays.asList(new Callable[] {transactionGraphTask, graphTask}), true);
		
		Collection<TransactionGraph> graphs = null;
		GraphResultData graphResult = null;
		
		for (Object taskResult : taskResults) {
			  
			if (taskResult instanceof Collection<?>) {
				graphs = (Collection<TransactionGraph>)taskResult;
			} else if (taskResult instanceof GraphResultData) {
				graphResult = (GraphResultData)taskResult;
			}
		}
		
		NavigableMap<DateTime, KpiInterval> regressionIntervals = processRegressions(serviceId, 
				viewId, input, kpi, timespan, periods, graphResult);
		
		NavigableMap<DateTime, KpiInterval> slowdownIntervals = processSlowdowns(serviceId, 
				viewId, input, periods, graphs);
		
		NavigableMap<DateTime, KpiInterval> intervals = getScoreIntervals(serviceId, 
			isKey, regressionIntervals, slowdownIntervals);
		
		KpiInterval aggregate;
		
		if (input.aggregate) {
			aggregate = aggregateScores(intervals);
		} else {
			KpiInterval lastInterval = intervals.lastEntry().getValue();
			intervals.put(timespan.getSecond(), lastInterval);
			aggregate = null;
		}
		
		return Pair.of(intervals, aggregate);		

	}
	
	private Pair<Map<DateTime, KpiInterval>, KpiInterval> processVolumes(
			String serviceId, String viewId, ReliabilityKpiGraphInput input, 
			Pair<DateTime, DateTime> timespan, Collection<Pair<DateTime, DateTime>> periods) {
	
		Map<DateTime, KpiInterval> intervals = processVolumesMap(serviceId, viewId, input, timespan, periods);
		
		KpiInterval aggregate;
		
		if (input.aggregate) {
			aggregate = aggregateVolumes(intervals);
		} else {
			aggregate = null;
		}
		
		return Pair.of(intervals, aggregate);		
	}
	
	@SuppressWarnings("unchecked")
	private Pair<Map<DateTime, KpiInterval>, KpiInterval> processSlowdowns(
			String serviceId, String viewId, ReliabilityKpiGraphInput input, 
			Pair<DateTime, DateTime> timespan, Collection<Pair<DateTime, DateTime>> periods) {
	
		TransactionGraphTask txGraphTask = new TransactionGraphTask(serviceId, viewId, input, timespan);
		
		Collection<TransactionGraph> graphs;
		
		try {
			graphs = (Collection<TransactionGraph>)txGraphTask.call();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}

		Map<DateTime, KpiInterval> intervals = processSlowdowns(serviceId, viewId, input, periods, graphs);
		
		KpiInterval aggregate;

		if (input.aggregate) {
			aggregate = aggregateSlowdowns(intervals);
		} else {
			aggregate = null;
		}
		
		return Pair.of(intervals, aggregate);		
	}
	
	protected List<GraphSeries> processServiceGraph(Collection<String> serviceIds,
		String serviceId, String viewId, BaseGraphInput input, 
		Pair<DateTime, DateTime> timespan, Collection<Pair<DateTime, DateTime>> periods,
		String app, boolean isKey) {
		
		ReliabilityKpiGraphInput rkInput =  (ReliabilityKpiGraphInput)input;
		
		ReliabilityKpi kpi = rkInput.getKpi();
		
		Pair<Map<DateTime, KpiInterval>, KpiInterval> intervalPair;
		
		switch (kpi) {
			
			case NewErrors:
			case SevereNewErrors:
			case IncreasingErrors:
			case SevereIncreasingErrors:
				
				intervalPair = processRegressions(serviceId, viewId, rkInput, timespan, periods, kpi);			
				break;
			
			case Slowdowns:
			case SevereSlowdowns:
			
				intervalPair = processSlowdowns(serviceId, viewId, rkInput, timespan, periods);
				break;
							
			case ErrorVolume:		
			case ErrorCount:
			case ErrorRate:
				
				intervalPair = processVolumes(serviceId, viewId, rkInput, timespan, periods);
				break;
				
			case Score:
				
				intervalPair = processScores(serviceId, viewId, rkInput, timespan, periods, isKey, kpi);
				break;
				
			default:
				throw new IllegalStateException(String.valueOf(kpi));
		}
		
		Map<DateTime, KpiInterval> intervalMap = intervalPair.getFirst();
		KpiInterval aggregate = intervalPair.getSecond();
		
		Map<DateTime, KpiInterval> targetIntervals;
		
		if (aggregate != null) {
			targetIntervals = Collections.singletonMap(timespan.getSecond(), aggregate);
		} else {
			targetIntervals = intervalMap;
		}
		
		List<GraphSeries> result = getGraphSeries(serviceIds, serviceId, 
			rkInput, kpi, targetIntervals, app);
		
		return result;
	}
	
	private List<GraphSeries> getGraphSeries(Collection<String> serviceIds,
		String serviceId, ReliabilityKpiGraphInput input,
		ReliabilityKpi kpi, Map<DateTime, KpiInterval> intervals,
		String app) {
		
		Series series = new Series();
		
		String tagName = getSeriesName(input, app, serviceId, serviceIds);
		String cleanTagName = cleanSeriesName(tagName);
		
		series.name = EMPTY_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, cleanTagName });
		series.values = new ArrayList<List<Object>>(intervals.size());
		
		for (Map.Entry<DateTime, KpiInterval> entry : intervals.entrySet()) {
			
			Object timeValue = getTimeValue(entry.getKey().getMillis(), input);
			Object seriesValue = entry.getValue().getValue(kpi);
			
			series.values.add(Arrays.asList(new Object[] {timeValue, seriesValue }));
		}
		
		return Collections.singletonList(GraphSeries.of(series, intervals.size(), app));
	}
	
	private NavigableMap<DateTime, KpiInterval> getScoreIntervals(String serviceId, 
		boolean isKey, Map<DateTime, KpiInterval> regressionIntervals, 
		Map<DateTime, KpiInterval> slowdownIntervals) {
		
		RegressionReportSettings reportSettings = GrafanaSettings.getData(apiClient, serviceId).regression_report;
		
		if (reportSettings == null) {
			throw new IllegalStateException("Unable to acquire regression report settings for " + serviceId);
		}	
		
		NavigableMap<DateTime, KpiInterval> result = new TreeMap<DateTime, KpiInterval>();
		
		for (Map.Entry<DateTime, KpiInterval> entry : regressionIntervals.entrySet()) {
			
			ScoreInterval scoreInterval = (ScoreInterval)(result.get(entry.getKey()));
			
			if (scoreInterval == null) {
				scoreInterval = new ScoreInterval();
				result.put(entry.getKey(), scoreInterval);
			}
			
			scoreInterval.regressionInterval = (RegressionInterval)(entry.getValue());
		}
		
		for (Map.Entry<DateTime, KpiInterval> entry : slowdownIntervals.entrySet()) {
			
			ScoreInterval scoreInterval = (ScoreInterval)(result.get(entry.getKey()));
			
			if (scoreInterval == null) {
				scoreInterval = new ScoreInterval();
				result.put(entry.getKey(), scoreInterval);
			}
			
			scoreInterval.slowdownInterval = (SlowdownInterval)(entry.getValue());
		}
		
		for (KpiInterval kpiInterval : result.values()) {
			
			ScoreInterval scoreInterval = (ScoreInterval)kpiInterval;
			
			if ((scoreInterval.regressionInterval == null) 
			|| (scoreInterval.slowdownInterval == null)) {
				continue;
			}
				
			Pair<Double, Integer> scorePair = ReliabilityReportFunction.getScore(
				scoreInterval.regressionInterval.output, reportSettings, 
				scoreInterval.regressionInterval.newErrors, 
				scoreInterval.regressionInterval.severeNewErrors, 
				scoreInterval.regressionInterval.regressions, 
				scoreInterval.regressionInterval.severeRegressions, 
				scoreInterval.slowdownInterval.slowdowns, 
				scoreInterval.slowdownInterval.severeSlowdowns, 
				isKey, ReportMode.Applications);
			
			scoreInterval.score = scorePair.getFirst().doubleValue();
		}
		
		return result;
	}
	
	@Override
	protected Collection<Callable<Object>> getTasks(Collection<String> serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {
		
		List<Callable<Object>> result = new ArrayList<Callable<Object>>();

		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		
		ReliabilityKpiGraphInput rkInput =  (ReliabilityKpiGraphInput)input;
				
		String timeUnit = TimeUtil.getTimeUnit(input.timeFilter);
		int reportInterval = TimeUtil.parseInterval(rkInput.reportInterval);
		
		Collection<Pair<DateTime, DateTime>> periods = getIntervalPeriods(timeSpan, reportInterval);
		
		Gson gson = new Gson();
		String json = gson.toJson(input);
		
		AppsGraphFunction appsFunction = new AppsGraphFunction(apiClient);
		
		for (String serviceId : serviceIds) {
			
			List<String> keyApps = new ArrayList<String>();
			
			GroupSettings appGroups = GrafanaSettings.getData(apiClient, serviceId).applications;
			
			if (appGroups != null) {
				keyApps.addAll(appGroups.getAllGroupNames(true));
			}
			
			String viewId = getViewId(serviceId, input.view);
			
			Pair<RegressionInput, RegressionWindow> regInput = regressionFunction.getRegressionInput(serviceId, 
					viewId, input, Pair.of(timeSpan.getSecond().minusMinutes(reportInterval), timeSpan.getSecond()), false);
				
			int baselineTimespan = regInput.getFirst().baselineTimespan;
			
			String expandedTimeFilter;

			if (timeUnit != null) {
				
				int interval = TimeUtil.parseInterval(timeUnit);
				int expandedInterval = interval + baselineTimespan;
				
				expandedTimeFilter = TimeUtil.getLastWindowMinTimeFilter(expandedInterval);
			} else {
				expandedTimeFilter = TimeUtil.toTimeFilter(timeSpan.getFirst().minusMinutes(baselineTimespan), 
					timeSpan.getSecond());
			}
					
			Pair<DateTime, DateTime> expandedTimeSpan = TimeUtil.getTimeFilter(expandedTimeFilter);
			
			Collection<String> apps = appsFunction.getApplications(serviceId, input, rkInput.limit);
			
			for (String app : apps) {
				
				ReliabilityKpiGraphInput appInput = gson.fromJson(json, rkInput.getClass());
				
				appInput.applications = app;
				appInput.timeFilter = expandedTimeFilter;
				
				boolean isKey = keyApps.contains(app);
				
				KpiGraphAsyncTask graphAsyncTask = new KpiGraphAsyncTask(serviceId, viewId, input.view, 
						appInput, expandedTimeSpan, serviceIds, pointsWanted, app, isKey, periods);
				
				result.add(graphAsyncTask);		
			}		
		}
		
		return result;
	}
	
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof ReliabilityKpiGraphInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
	}

	@Override
	protected List<GraphSeries> processServiceGraph(Collection<String> serviceIds, String serviceId, String viewId,
			String viewName, BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, int pointsWanted) {
		
		throw new IllegalStateException();
	}
}
