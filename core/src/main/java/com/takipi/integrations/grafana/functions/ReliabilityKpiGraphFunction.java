package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.api.client.util.settings.GroupSettings;
import com.takipi.api.client.util.settings.RegressionReportSettings;
import com.takipi.api.client.util.transaction.TransactionUtil;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.EventsFunction.EventData;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionData;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.ReliabilityKpiGraphInput;
import com.takipi.integrations.grafana.input.ReliabilityReportInput;
import com.takipi.integrations.grafana.input.ReliabilityReportInput.RelabilityKpi;
import com.takipi.integrations.grafana.input.ReliabilityReportInput.ReportMode;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.ServiceSettings;
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
	
	protected class TimelineData {
		
		protected int baselineWindow;
		
		protected ReliabilityKpiGraphInput input;
		protected Pair<DateTime, DateTime> timespan;
		
		protected ReliabilityKpiGraphInput expandedInput;
		protected Pair<DateTime, DateTime> expandedTimespan;
	
		protected ReliabilityKpiGraphInput baselineInput;
		protected Pair<DateTime, DateTime> baselineTimespan;
	}
	
	protected class GraphResultData {
		protected Graph graph;
		protected Map<String, EventResult> eventListMap;
	}
	
	protected abstract class BaseGraphAsyncTask extends BaseAsyncTask {
		
		protected String serviceId;
		protected String viewId;
		protected TimelineData timelineData;
	
		
		protected BaseGraphAsyncTask(String serviceId, String viewId, 
				TimelineData timelineData) {
			
			this.serviceId = serviceId;
			this.viewId = viewId;
			this.timelineData = timelineData;
		}
	}
	
	protected class GraphDataTask extends BaseGraphAsyncTask implements Callable<Object>{

		protected GraphDataTask(String serviceId, String viewId, TimelineData timelineData) {
			super(serviceId, viewId, timelineData);
		}
		
		@Override
		public Object call() throws Exception {
			
			GraphResultData result = new GraphResultData();

			result.graph = getEventsGraph(serviceId, viewId, timelineData.input.pointsWanted, 
					timelineData.expandedInput, timelineData.input.volumeType, 
					timelineData.expandedTimespan.getFirst(),
					timelineData.expandedTimespan.getSecond(), 0, 0, false);
			
			result.eventListMap = getEventMap(serviceId, timelineData.expandedInput,
					timelineData.timespan.getFirst(), timelineData.timespan.getSecond(), 
				null, timelineData.input.pointsWanted);
						
			return result;
		}
	}
	
	protected class TransactionResultData {
		protected boolean isBaseline;
		protected Collection<TransactionGraph> graphs;
	}
	
	protected class TasksResultData {
		protected TransactionResultData baselineTransactionData;
		protected TransactionResultData activeTransactionData;
		protected GraphResultData graphResultData;
	}
		
	protected class TransactionGraphTask extends BaseGraphAsyncTask implements Callable<Object>{
			
		protected TransactionGraphTask(String serviceId, String viewId, 
				TimelineData timelineData) {
			
			super(serviceId, viewId, timelineData);
		}
		
		@Override
		public Object call() throws Exception {
					
			TransactionResultData result = new TransactionResultData();

			result.graphs = getTransactionGraphs(timelineData.input, serviceId, viewId, 
					timelineData.timespan, timelineData.input.getSearchText(), 
					timelineData.input.transactionPointsWanted);
			
			return result;
		}
	}
	
	protected class TransactionBaselineGraphTask extends BaseGraphAsyncTask implements Callable<Object>{
		
		
		protected TransactionBaselineGraphTask(String serviceId, String viewId, 
				TimelineData timelineData) {
			
			super(serviceId, viewId, timelineData);
		}
		
		@Override
		public Object call() throws Exception {
			
			TransactionResultData result = new TransactionResultData();

			result.isBaseline = true;
			result.graphs = getTransactionGraphs(timelineData.baselineInput, serviceId, viewId, 
					timelineData.baselineTimespan, timelineData.baselineInput.getSearchText(), 
					timelineData.baselineInput.transactionPointsWanted, 0, timelineData.baselineWindow);
						
			return result;
		}
	}
	
	public class TaskKpiResult {
		protected String app;
		protected Collection<KpiInterval> kpiIntervals;
		
		protected TaskKpiResult(String app, Collection<KpiInterval> kpiIntervals) {
			this.app = app;
			this.kpiIntervals = kpiIntervals;
		}
	}
	
	protected class KpiGraphAsyncTask extends GraphAsyncTask {
		
		protected String app;
		protected boolean isKey;
		protected Collection<Pair<DateTime, DateTime>> periods;
		protected boolean returnKpi;
		protected TimelineData timelineData;
		
		protected KpiGraphAsyncTask(String serviceId, String viewId, String viewName,
				TimelineData timelineData, Collection<String> serviceIds, int pointsWanted,
				String app, boolean isKey, Collection<Pair<DateTime, DateTime>> periods,
				boolean returnKpi) {

			super(serviceId, viewId, viewName, timelineData.input, timelineData.timespan,
				serviceIds, pointsWanted);
			this.app = app;
			this.isKey = isKey;
			this.periods = periods;
			this.returnKpi = returnKpi;
			this.timelineData = timelineData;
		}

		@Override
		public Object call() {
			
			beforeCall();
			
			try {
				
				ReliabilityKpiGraphInput rkInput =  (ReliabilityKpiGraphInput)input;
				RelabilityKpi kpi = ReliabilityReportInput.getKpi(rkInput.kpi);

				Map<DateTime, KpiInterval> kpiIntervals = processServiceKpis(serviceId, viewId, 
						rkInput, timelineData, kpi, periods, isKey);
				
				Object result;
				
				if (returnKpi) {
					result = new TaskKpiResult(app, kpiIntervals.values());
				} else {
					List<GraphSeries> series = getGraphSeries(serviceIds, serviceId, 
						rkInput, kpi, kpiIntervals, app);
					
					result = new TaskSeriesResult(series);
				}
								
				return result;
			} finally {
				afterCall();
			}
		
		}
		
		@Override
		public String toString() {
			return String.join(" ", "KPI", serviceId, viewId, app, 
				timeSpan.toString(), String.valueOf(pointsWanted));
		}
	}
	
	public abstract class KpiInterval {
		
		public Pair<DateTime, DateTime> period;
		
		protected abstract Object getValue(RelabilityKpi kpi);
		
		protected KpiInterval(Pair<DateTime, DateTime> period) {
			this.period = period;
		}
	}
	
	public class RegressionInterval extends KpiInterval {
		
		protected int newErrors;
		protected int severeNewErrors;
		protected int regressions;
		protected int severeRegressions;
		protected RegressionOutput output;
		
		protected RegressionInterval(Pair<DateTime, DateTime> period) {
			super(period);
		}
		
		@Override
		protected Object getValue(RelabilityKpi kpi) {
			
			switch (kpi) {
				
				case NewErrors: 
					return newErrors + severeNewErrors;
				
				case SevereNewErrors: 
					return severeNewErrors;
				
				case ErrorIncreases: 
					return regressions + severeRegressions;
				
				case SevereErrorIncreases: 
					return severeRegressions;
				
				default: throw new IllegalStateException(kpi.toString());
			}
		}
	}
	
	public class SlowdownInterval extends KpiInterval {
		
		protected int slowdowns;
		protected int severeSlowdowns;
		protected Map<TransactionKey, TransactionData> transactionMap;
		protected RegressionInput regressionInput;
		
		protected SlowdownInterval(Pair<DateTime, DateTime> period) {
			super(period);
		}
		
		@Override
		protected Object getValue(RelabilityKpi kpi) {
			
			switch (kpi) {
				
				case Slowdowns: 
					return slowdowns + severeSlowdowns;
					
				case SevereSlowdowns: 
					return severeSlowdowns;
					
				default: throw new IllegalStateException(kpi.toString());
			}
		}
	}
	
	public class VolumeInterval extends KpiInterval  {
		
		protected long volume;
		protected long invocations;
		protected Set<String> eventIds;
		

		protected VolumeInterval(Pair<DateTime, DateTime> period) {
			super(period);
			this.eventIds = new HashSet<String>();
		}
		
		@Override
		protected Object getValue(RelabilityKpi kpi) {
			
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
	}
	
	public class ScoreInterval extends KpiInterval {
		
		protected ScoreInterval(Pair<DateTime, DateTime> period) {
			super(period);
		}
		
		protected RegressionInterval regressionInterval;
		protected SlowdownInterval slowdownInterval;
		protected double score;
		
		@Override
		protected Object getValue(RelabilityKpi kpi) {
			return score;
		}
	}

	public ReliabilityKpiGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	public ReliabilityKpiGraphFunction(ApiClient apiClient, Map<String, ServiceSettings> settingsMaps) {
		super(apiClient, settingsMaps);
	}
	
	private Collection<TransactionGraph> sectionGraphs(Collection<TransactionGraph> graphs,
		DateTime start, DateTime end) {
		
		List<TransactionGraph> result = new ArrayList<TransactionGraph>();
		
		for (TransactionGraph graph : graphs) {
			
			TransactionGraph resultGraph = new TransactionGraph();
				
			resultGraph.class_name = graph.class_name;
			resultGraph.method_desc = graph.method_desc;
			resultGraph.method_name = graph.method_name;
			resultGraph.name = graph.name;
			resultGraph.points = new ArrayList<com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint>();
				
			result.add(resultGraph);
				
			for (com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint gp : graph.points) {
					
				DateTime gpTime = TimeUtil.getDateTime(gp.time);
					
				if (timespanContains(start, end, gpTime)) {
					resultGraph.points.add(gp);
				} 
			}					
		}
		
		return result;
		
	}
	
	private Pair<Collection<TransactionGraph>, Collection<TransactionGraph>> getSlowdownGraphs(
			Collection<TransactionGraph> baselineGraphs, 
			Collection<TransactionGraph> activeGraphs, 
			Pair<DateTime, DateTime> period, int baselineWindow) {
			
		DateTime baselineEnd = period.getFirst();
		DateTime baselineStart = baselineEnd.minusMinutes(baselineWindow);
		
		Collection<TransactionGraph> resultBaselineGraphs = sectionGraphs(baselineGraphs, baselineStart, baselineEnd);
		Collection<TransactionGraph> resultActiveGraphs = sectionGraphs(activeGraphs, period.getFirst(), period.getSecond());
		
		return Pair.of(resultBaselineGraphs, resultActiveGraphs);
	}
		
	private NavigableMap<DateTime, KpiInterval> processSlowdowns(String serviceId,
		String viewId, TimelineData timelineData, 
		Collection<Pair<DateTime, DateTime>> periods, 
		Collection<TransactionGraph> baselineGraphs, 
		Collection<TransactionGraph> activeGraphs) {
		
		if ((baselineGraphs == null) || (activeGraphs == null)) {
			return Collections.emptyNavigableMap();
		}

		RegressionFunction regressionFunction = new RegressionFunction(apiClient, settingsMaps);
		
		NavigableMap<DateTime, KpiInterval> result = new TreeMap<DateTime, KpiInterval>();
		
		for (Pair<DateTime, DateTime> period : periods) {
			
			Pair<RegressionInput, RegressionWindow> regPair = regressionFunction.getRegressionInput(serviceId, viewId,
					timelineData.input, period, true);
			
			RegressionInput regressionInput = regPair.getFirst();
			
			Pair<Collection<TransactionGraph>, Collection<TransactionGraph>> graphPairs = getSlowdownGraphs(baselineGraphs,
				activeGraphs, period, regressionInput.baselineTimespan);
						
			Map<String, TransactionGraph> baselineGraphsMap = TransactionUtil.getTransactionGraphsMap(graphPairs.getFirst());	
			Map<String, TransactionGraph> activeGraphsMap = TransactionUtil.getTransactionGraphsMap(graphPairs.getSecond());

			Map<TransactionKey, TransactionData> transactionsMap = getTransactionDatas(serviceId,
				activeGraphsMap.values(), baselineGraphsMap.values());
			
			SlowdownInterval slowdownInterval = new SlowdownInterval(period);
			
			slowdownInterval.transactionMap = transactionsMap;
			slowdownInterval.regressionInput = regressionInput;
					
			for (TransactionData transactionData : transactionsMap.values()) {
			
				switch (transactionData.state) {
					
					case CRITICAL:
						slowdownInterval.severeSlowdowns++;
						break;
					
					case SLOWING:
						slowdownInterval.slowdowns++;
						break;
						
					default: continue;
				}	
			}
			
			result.put(period.getSecond(), slowdownInterval);				
		}
		
		return result;
	}
	
	private Map<DateTime, KpiInterval> processVolumesMap(String serviceId,
			String viewId, TimelineData timelineData,
			Collection<Pair<DateTime, DateTime>> periods) {
		
		Graph graph = getEventsGraph(serviceId, viewId, timelineData.input.pointsWanted, 
				timelineData.input, timelineData.input.volumeType, 
				timelineData.timespan.getFirst(), timelineData.timespan.getSecond());
		
		if (graph == null) {
			return Collections.emptyMap();
		}
		
		EventFilter eventFilter = getEventFilter(serviceId, timelineData.input, timelineData.timespan);

		if (eventFilter == null) {
			return Collections.emptyMap();
		}
		
		Map<String, EventResult> eventMap = getEventMap(serviceId, timelineData.input,
			timelineData.timespan.getFirst(), timelineData.timespan.getSecond(), 
			null, timelineData.input.pointsWanted);
		
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
						volumeInterval = new VolumeInterval(period);
						result.put(period.getSecond(), volumeInterval);
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
	
	private KpiInterval aggregateRegressions(Pair<DateTime, DateTime> timespan,
			Map<DateTime, KpiInterval> intervals) {
		
		RegressionInterval result = new RegressionInterval(timespan);
		
		for (KpiInterval kpiInterval : intervals.values()) {
			
			RegressionInterval regressionInterval = (RegressionInterval)kpiInterval;
			
			result.newErrors += regressionInterval.newErrors;
			result.severeNewErrors += regressionInterval.severeNewErrors;
			result.regressions += regressionInterval.regressions;
			result.severeRegressions += regressionInterval.severeRegressions;
		}
		
		return result;
	}
	
	private KpiInterval aggregateSlowdowns(Pair<DateTime, DateTime> timespan, Map<DateTime, KpiInterval> intervals) {
		
		SlowdownInterval result = new SlowdownInterval(timespan);
		
		for (KpiInterval kpiInterval : intervals.values()) {
			
			SlowdownInterval slowdownInterval = (SlowdownInterval)kpiInterval;
			
			result.slowdowns += slowdownInterval.slowdowns;
			result.severeSlowdowns += slowdownInterval.severeSlowdowns;
		}
		
		return result;
	}
	
	private KpiInterval aggregateScores(Pair<DateTime, DateTime> timespan,
		Map<DateTime, KpiInterval> intervals) {
		
		ScoreInterval result = new ScoreInterval(timespan);
		
		for (KpiInterval kpiInterval : intervals.values()) {
			
			ScoreInterval scoreInterval = (ScoreInterval)kpiInterval;
			
			result.score += scoreInterval.score;
		}
		
		if (intervals.size() > 0) {
			result.score /= intervals.size();
		}
		
		return result;
	}
	
	private KpiInterval aggregateVolumes(Pair<DateTime, DateTime> timespan,
		Map<DateTime, KpiInterval> intervals) {
		
		VolumeInterval result = new VolumeInterval(timespan);
		
		for (KpiInterval kpiInterval : intervals.values()) {
			
			VolumeInterval volumeInterval = (VolumeInterval)kpiInterval;
			
			result.volume += volumeInterval.volume;
			result.invocations += volumeInterval.invocations; 
			result.eventIds.addAll(volumeInterval.eventIds);
		}
				
		return result;	
	}
	
	private NavigableMap<DateTime, KpiInterval> processRegressions(String serviceId,
			String viewId, TimelineData timelineData, RelabilityKpi kpi,
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
		
		RegressionFunction regressionFunction = new RegressionFunction(apiClient, settingsMaps);
		
		boolean newOnly = (kpi == RelabilityKpi.NewErrors) || (kpi == RelabilityKpi.SevereNewErrors);
		
		NavigableMap<DateTime, KpiInterval> result = new TreeMap<DateTime, KpiInterval>();
		
		for (Pair<DateTime, DateTime> period : periods) {
				
			RegressionWindow inputWindow = new RegressionWindow();
			
			inputWindow.activeTimespan = (int)TimeUnit.MILLISECONDS.toMinutes(period.getSecond().getMillis() - period.getFirst().getMillis());
			inputWindow.activeWindowStart = period.getFirst();
			
			Pair<RegressionInput, RegressionWindow> regPair = regressionFunction.getRegressionInput(serviceId, viewId,
				timelineData.input, inputWindow, period, newOnly);
			
			RegressionInput regressionInput = regPair.getFirst();
			RegressionWindow regressionWindow = regPair.getSecond();
			
			Pair<Map<String, EventResult>, Long> filteredResult = filterEvents(serviceId, 
				period, timelineData.input, graphData.eventListMap.values());

			RegressionPeriodData regressionPeriodData = getRegressionGraphs(
				graphData.graph, period, regressionInput.baselineTimespan, filteredResult.getFirst());
			
			Graph baselineGraph = regressionPeriodData.baselineGraph;
			Graph activeGraph = regressionPeriodData.activeGraph;
			
			Collection<EventResult> clonedEvents = cloneEvents(regressionPeriodData.eventMap.values(), false);
			Map<String, EventResult> clonedEventsMap = getEventsMap(clonedEvents);
			
			long volume = applyGraphToEvents(clonedEventsMap, activeGraph, null);
			
			Map<String, EventResult> nonEmptyEventsMap = getEventsMap(clonedEventsMap.values(), true);
						
			regressionInput.events = nonEmptyEventsMap.values();
			regressionInput.baselineGraph = baselineGraph;
			
			RegressionOutput regressionOutput = regressionFunction.executeRegression(serviceId,
				timelineData.input, regressionInput, regressionWindow, 
				nonEmptyEventsMap, volume, baselineGraph, activeGraph, true);					
			
			RegressionInterval regressionInterval = new RegressionInterval(period);
			
			regressionInterval.output = regressionOutput;
						
			for (EventData eventData : regressionOutput.eventDatas) {
				
				RegressionData regData = (RegressionData)eventData;
				
				if (regData.event.stats.hits == 0) {
					//continue;
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
			
			result.put(period.getSecond(), regressionInterval);
		}
		
		return result;
	}
	
	private Pair<Map<DateTime, KpiInterval>, KpiInterval> processRegressions(
		String serviceId, String viewId, TimelineData timelineData, 
		Collection<Pair<DateTime, DateTime>> periods, RelabilityKpi kpi) {
		
		GraphDataTask graphTask = new GraphDataTask(serviceId, viewId, timelineData);
		
		GraphResultData graphResult;
		
		try {
			graphResult = (GraphResultData)(graphTask.call());
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		
		Map<DateTime, KpiInterval> intervals = processRegressions(serviceId, 
			viewId, timelineData, kpi, periods, graphResult);
		
		KpiInterval aggregate;
		
		if (timelineData.input.aggregate) {
			aggregate = aggregateRegressions(timelineData.timespan, intervals);
		} else {
			aggregate = null;	
		}
		
		return Pair.of(intervals, aggregate);
	}
	
	private TasksResultData getTaskResults(List<Object> taskResults ) {
		
		TasksResultData result = new TasksResultData();
		
		for (Object taskResult : taskResults) {
			  
			if (taskResult instanceof TransactionResultData) {
				TransactionResultData transactionResult = (TransactionResultData)taskResult;
				
				if (transactionResult.isBaseline) {
					result.baselineTransactionData = transactionResult;
				} else {
					result.activeTransactionData = transactionResult;
				}
				
			} else if (taskResult instanceof GraphResultData) {
				result.graphResultData = (GraphResultData)taskResult;
			}
		}
		
		return result;
		
	}
	
	@SuppressWarnings("unchecked")
	private Pair<Map<DateTime, KpiInterval>, KpiInterval> processScores(
			String serviceId, String viewId, ReliabilityKpiGraphInput input, 
			TimelineData timelineData, Collection<Pair<DateTime, DateTime>> periods, 
			boolean isKey, RelabilityKpi kpi) {
	
		TransactionGraphTask transactionGraphTask = new TransactionGraphTask(serviceId, viewId, timelineData);
		
		TransactionBaselineGraphTask baselineGraphTask = new TransactionBaselineGraphTask(serviceId, 
			viewId, timelineData);

		GraphDataTask graphTask = new GraphDataTask(serviceId, viewId, timelineData);

		List<Object> taskResults = executeTasks(Arrays.asList(new Callable[] 
			{transactionGraphTask, baselineGraphTask, graphTask}), true);
		
		TasksResultData tasksResultData = getTaskResults(taskResults);
		
		NavigableMap<DateTime, KpiInterval> regressionIntervals = processRegressions(serviceId, 
				viewId, timelineData, kpi, periods, tasksResultData.graphResultData);
		
		NavigableMap<DateTime, KpiInterval> slowdownIntervals = processSlowdowns(serviceId, 
				viewId, timelineData, periods, tasksResultData.baselineTransactionData.graphs,
				tasksResultData.activeTransactionData.graphs);
		
		NavigableMap<DateTime, KpiInterval> intervals = getScoreIntervals(serviceId, 
			input, isKey, timelineData.input.deductFrom100, 
			regressionIntervals, slowdownIntervals);
		
		KpiInterval aggregate;
		
		if (timelineData.input.aggregate) {
			aggregate = aggregateScores(timelineData.timespan, intervals);
		} else {
			aggregate = null;
		}
		
		return Pair.of(intervals, aggregate);		

	}
	
	private Pair<Map<DateTime, KpiInterval>, KpiInterval> processVolumes(
			String serviceId, String viewId, TimelineData timelineData, 
			Collection<Pair<DateTime, DateTime>> periods) {
	
		Map<DateTime, KpiInterval> intervals = processVolumesMap(serviceId, viewId, timelineData, periods);
		
		KpiInterval aggregate;
		
		if (timelineData.input.aggregate) {
			aggregate = aggregateVolumes(timelineData.timespan, intervals);
		} else {
			aggregate = null;
		}
		
		return Pair.of(intervals, aggregate);		
	}
	
	@SuppressWarnings("unchecked")
	private Pair<Map<DateTime, KpiInterval>, KpiInterval> processSlowdowns(
			String serviceId, String viewId, TimelineData timelineData, Collection<Pair<DateTime, DateTime>> periods) {
	
		TransactionGraphTask transactionGraphTask = new TransactionGraphTask(serviceId, viewId, timelineData);
		
		TransactionBaselineGraphTask baselineGraphTask = new TransactionBaselineGraphTask(serviceId, 
			viewId, timelineData);

		GraphDataTask graphTask = new GraphDataTask(serviceId, viewId, timelineData);

		List<Object> taskResults = executeTasks(Arrays.asList(new Callable[] 
			{transactionGraphTask, baselineGraphTask, graphTask}), true);		
		
		TasksResultData tasksResultData = getTaskResults(taskResults);
		
		Map<DateTime, KpiInterval> intervals = processSlowdowns(serviceId, viewId, timelineData, 
			periods, tasksResultData.baselineTransactionData.graphs,
			tasksResultData.activeTransactionData.graphs);
		
		KpiInterval aggregate;

		if (timelineData.input.aggregate) {
			aggregate = aggregateSlowdowns(timelineData.timespan, intervals);
		} else {
			aggregate = null;
		}
		
		return Pair.of(intervals, aggregate);		
	}
	
	private Map<DateTime, KpiInterval> processServiceKpis(String serviceId, String viewId, 
		ReliabilityKpiGraphInput input, TimelineData timelineData, RelabilityKpi kpi,
		Collection<Pair<DateTime, DateTime>> periods, boolean isKey) {
						
		Pair<Map<DateTime, KpiInterval>, KpiInterval> intervalPair;
		
		switch (kpi) {
			
			case NewErrors:
			case SevereNewErrors:
			case ErrorIncreases:
			case SevereErrorIncreases:
				
				intervalPair = processRegressions(serviceId, viewId, timelineData, periods, kpi);			
				break;
			
			case Slowdowns:
			case SevereSlowdowns:
			
				intervalPair = processSlowdowns(serviceId, viewId, timelineData, periods);
				break;
							
			case ErrorVolume:		
			case ErrorCount:
			case ErrorRate:
				
				intervalPair = processVolumes(serviceId, viewId, timelineData, periods);
				break;
				
			case Score:
				
				intervalPair = processScores(serviceId, viewId, input, timelineData, periods, isKey, kpi);
				break;
				
			default:
				throw new IllegalStateException(String.valueOf(kpi));
		}
		
		Map<DateTime, KpiInterval> intervalMap = intervalPair.getFirst();
		KpiInterval aggregate = intervalPair.getSecond();
		
		Map<DateTime, KpiInterval> result;
		
		if (aggregate != null) {
			result = Collections.singletonMap(timelineData.timespan.getSecond(), aggregate);
		} else {
			result = intervalMap;
		}
		
		return result;
	}
	
	private List<GraphSeries> getGraphSeries(Collection<String> serviceIds,
		String serviceId, ReliabilityKpiGraphInput input,
		RelabilityKpi kpi, Map<DateTime, KpiInterval> intervals,
		String app) {
		
		String tagName = getSeriesName(input, app, serviceId, serviceIds);
		String cleanTagName = cleanSeriesName(tagName);
		
		Series series = createGraphSeries(cleanTagName, 0);
		
		for (Map.Entry<DateTime, KpiInterval> entry : intervals.entrySet()) {
			
			Object timeValue = getTimeValue(entry.getKey().getMillis(), input);
			Object seriesValue = entry.getValue().getValue(kpi);
			
			series.values.add(Arrays.asList(new Object[] {timeValue, seriesValue }));
		}
		
		return Collections.singletonList(GraphSeries.of(series, intervals.size(), app));
	}
	
	private NavigableMap<DateTime, KpiInterval> getScoreIntervals(String serviceId, 
		ReliabilityKpiGraphInput input, boolean isKey, boolean deductFrom100, 
		Map<DateTime, KpiInterval> regressionIntervals, 
		Map<DateTime, KpiInterval> slowdownIntervals) {
		
		RegressionReportSettings reportSettings = getSettingsData(serviceId).regression_report;
		
		if (reportSettings == null) {
			throw new IllegalStateException("Unable to acquire regression report settings for " + serviceId);
		}	
		
		NavigableMap<DateTime, KpiInterval> result = new TreeMap<DateTime, KpiInterval>();
		
		for (Map.Entry<DateTime, KpiInterval> entry : regressionIntervals.entrySet()) {
			
			RegressionInterval regressionInterval = (RegressionInterval)(entry.getValue());
			ScoreInterval scoreInterval = (ScoreInterval)(result.get(entry.getKey()));
			
			if (scoreInterval == null) {
				scoreInterval = new ScoreInterval(regressionInterval.period);
				result.put(entry.getKey(), scoreInterval);
			}
			
			scoreInterval.regressionInterval = regressionInterval;
		}
		
		for (Map.Entry<DateTime, KpiInterval> entry : slowdownIntervals.entrySet()) {
			
			SlowdownInterval slowdownInterval =  (SlowdownInterval)(entry.getValue());
			ScoreInterval scoreInterval = (ScoreInterval)(result.get(entry.getKey()));
			
			if (scoreInterval == null) {
				scoreInterval = new ScoreInterval(slowdownInterval.period);
				result.put(entry.getKey(), scoreInterval);
			}
			
			scoreInterval.slowdownInterval = slowdownInterval;
		}
		
		for (KpiInterval kpiInterval : result.values()) {
			
			ScoreInterval scoreInterval = (ScoreInterval)kpiInterval;
			
			if ((scoreInterval.regressionInterval == null) 
			|| (scoreInterval.slowdownInterval == null)) {
				continue;
			}
				
			int appSize;
			Collection<String> apps = input.getApplications(apiClient, getSettingsData(serviceId), serviceId);
			
			if (CollectionUtil.safeIsEmpty(apps)) {
				appSize = apps.size();
			} else {
				appSize = 0;
			}
			
			Pair<Double, Integer> scorePair = ReliabilityReportFunction.getScore(
				scoreInterval.regressionInterval.output, reportSettings, 
				scoreInterval.regressionInterval.newErrors, 
				scoreInterval.regressionInterval.severeNewErrors, 
				scoreInterval.regressionInterval.regressions, 
				scoreInterval.regressionInterval.severeRegressions, 
				scoreInterval.slowdownInterval.slowdowns, 
				scoreInterval.slowdownInterval.severeSlowdowns, 
				isKey, deductFrom100, ReportMode.Applications, appSize);
			
			scoreInterval.score = scorePair.getFirst().doubleValue();
		}
		
		return result;
	}
	
	public Collection<TaskKpiResult> executeIntervals(Collection<String> serviceIds, 
			ReliabilityKpiGraphInput input, Pair<DateTime, DateTime> timeSpan) {
		
		Collection<Callable<Object>> tasks = getTasks(serviceIds, input, 
			timeSpan, input.pointsWanted, false, true);
		
		List<Object> taskResults;
		
		if (tasks.size() > 1) {
			taskResults	= executeTasks(tasks, true);
		} else {
			try {
				taskResults = Collections.singletonList(tasks.iterator().next().call());
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		
		List<TaskKpiResult> result = new ArrayList<TaskKpiResult>();
		
		for (Object taskResult : taskResults) {
			
			if (taskResult instanceof TaskKpiResult) {
				result.add((TaskKpiResult)taskResult);
			}
		}
		
		return result;
	}
	
	@Override
	protected Collection<Callable<Object>> getTasks(Collection<String> serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {
		return getTasks(serviceIds, input, timeSpan, pointsWanted, true, false);
	}
	
	private TimelineData getTimelineData(ReliabilityKpiGraphInput input ,
			Pair<DateTime, DateTime> timeSpan,
			int baselineTimespan, Gson gson, String json, String app) {
	
		TimelineData result = new TimelineData();
		
		String expandedTimeFilter;

		String timeUnit = TimeUtil.getTimeUnit(input.timeFilter);

		if (timeUnit != null) {
			
			int interval = TimeUtil.parseInterval(timeUnit);
			int expandedInterval = interval + baselineTimespan;
			
			expandedTimeFilter = TimeUtil.getLastWindowMinTimeFilter(expandedInterval);
		} else {
			expandedTimeFilter = TimeUtil.toTimeFilter(timeSpan.getFirst().minusMinutes(baselineTimespan), 
				timeSpan.getSecond());
		}
		
		String apps;
		
		if (app != null) {
			apps = app;
		} else {
			apps = input.applications;
		}
		
		result.input = gson.fromJson(json, input.getClass());
		result.input.applications = apps;
		result.timespan = TimeUtil.getTimeFilter(result.input.timeFilter);
		
		result.expandedInput = gson.fromJson(json, input.getClass());
		result.expandedInput.applications = apps;
		result.expandedInput.timeFilter = expandedTimeFilter;		
		result.expandedTimespan = TimeUtil.getTimeFilter(expandedTimeFilter);
		
		result.baselineInput = gson.fromJson(json, input.getClass());
		result.baselineInput.applications = apps;
		result.baselineTimespan = Pair.of(result.expandedTimespan.getFirst(), result.timespan.getFirst());
		//result.baselineInput.timeFilter = TimeUtil.toTimeFilter(result.baselineTimespan);			
	
		return result;
		
	}
	
	protected Collection<Callable<Object>> getTasks(Collection<String> serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted, 
			boolean splitByApp, boolean returnKpi) {
		
		List<Callable<Object>> result = new ArrayList<Callable<Object>>();

		RegressionFunction regressionFunction = new RegressionFunction(apiClient, settingsMaps);
		
		ReliabilityKpiGraphInput rkInput =  (ReliabilityKpiGraphInput)input;
				
		TimeUtil.Interval reportInterval = rkInput.getReportInterval();
		
		Pair<DateTime, Integer> intervalPair = TimeUtil.getPeriodStart(timeSpan, reportInterval);
		
		DateTime periodStart = intervalPair.getFirst();
		int timeDelta = intervalPair.getSecond();
		
		Collection<Pair<DateTime, DateTime>> periods = TimeUtil.getTimespanPeriods(timeSpan, 
			periodStart, timeDelta, false);
		
		Gson gson = new Gson();
		String json = gson.toJson(input);
		
		AppsGraphFunction appsFunction = new AppsGraphFunction(apiClient);
		
		Pair<DateTime, DateTime> regInputwindow = Pair.of(timeSpan.getSecond().minusMinutes(timeDelta), timeSpan.getSecond());

		for (String serviceId : serviceIds) {
			
			String viewId = getViewId(serviceId, input.view);
			
			Pair<RegressionInput, RegressionWindow> regInput = regressionFunction.getRegressionInput(serviceId, 
					viewId, input, regInputwindow, false);
				
			int baselineTimespan = regInput.getFirst().baselineTimespan;
			
			List<String> keyApps = new ArrayList<String>();
			GroupSettings appGroups = getSettingsData(serviceId).applications;
			
			if (appGroups != null) {
				keyApps.addAll(appGroups.getAllGroupNames(true));
			}
						
			if (splitByApp) {
					
				Collection<String> apps = appsFunction.getApplications(serviceId, input, rkInput.limit);
				
				for (String app : apps) {
					
					TimelineData timelineData = getTimelineData(rkInput, timeSpan,
						baselineTimespan, gson, json, app);
					
					boolean isKey = keyApps.contains(app);
					
					KpiGraphAsyncTask graphAsyncTask = new KpiGraphAsyncTask(serviceId, viewId, input.view, 
							timelineData, serviceIds, pointsWanted, app, isKey, periods, returnKpi);
					
					result.add(graphAsyncTask);		
				}		
			} else {
				
				TimelineData timelineData = getTimelineData(rkInput, timeSpan,
						baselineTimespan, gson, json, null);
				
				Collection<String> inputApps = input.getApplications(apiClient, getSettingsData(serviceId), serviceId);
				
				boolean isKey = isInputKeyApps(keyApps, inputApps);
			 	
				KpiGraphAsyncTask graphAsyncTask = new KpiGraphAsyncTask(serviceId, viewId, 
					input.view, timelineData, serviceIds, 
					pointsWanted, null, isKey, periods, returnKpi);
				
				result.add(graphAsyncTask);	
			}
		}
		
		return result;
	}
	
	private boolean isInputKeyApps(Collection<String> keyApps,
		Collection<String> inputApps) {
		
		boolean result;
		
	 	if (CollectionUtil.safeIsEmpty(inputApps)) {
	 		result = false;
	 	} else {
	 		result = true;
	 		
	 		for (String inputApp : inputApps) {
		 		if (!keyApps.contains(inputApp)) {
		 			result = false;
		 			break;
		 		}
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
