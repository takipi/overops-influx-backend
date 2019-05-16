package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.result.metrics.GraphResult;
import com.takipi.api.client.util.regression.RateRegression;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionResult;
import com.takipi.api.client.util.regression.RegressionStringUtil;
import com.takipi.api.client.util.regression.RegressionUtil;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.api.client.util.settings.RegressionReportSettings;
import com.takipi.api.client.util.settings.RegressionSettings;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseEventVolumeInput;
import com.takipi.integrations.grafana.input.EventFilterInput;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.input.RegressionsInput.RegressionType;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.ServiceSettings;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.EventLinkEncoder;
import com.takipi.integrations.grafana.util.TimeUtil;

public class RegressionFunction extends EventsFunction {	

	public static class Factory implements FunctionFactory {
		
		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new RegressionFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass() {
			return RegressionsInput.class;
		}
		
		@Override
		public String getName() {
			return "regressions";
		}
	}
	
	protected class RegressionData extends EventData {
		
		protected RegressionType type;
		protected RegressionResult regression;
		protected RateRegression regResult;
		protected RegressionInput input;
		protected Set<String> mergedIds;
		
		protected RegressionData(RateRegression regResult, RegressionInput input,
				EventResult event, RegressionType type) {
			super(event);
			this.type = type;
			this.regResult = regResult;
			this.input = input;
			this.regression = null;
			this.mergedIds = new HashSet<String>();
		}
			
		protected RegressionData(RateRegression regResult, RegressionInput input,
				RegressionResult regression, RegressionType type) {
			this(regResult, input, regression.getEvent(), type);
			this.regression = regression;
		}
		
		protected int getDelta() {
			
			if (regression == null) {
				return 0;
			}
			
			double baselineRate = (double) regression.getBaselineHits() 
					/ (double) regression.getBaselineInvocations()  * 100;
				
			double activeRate = (double) event.stats.hits
					/ (double) event.stats.invocations  * 100;
				
			double delta = activeRate - baselineRate;
					
			return (int)(delta);
		}
		
		public String getDescription() {
			
			String ratio;
			
			if (event.stats.invocations > 0) {
				ratio = (decimalFormat.format((double)event.stats.hits / (double)event.stats.invocations));
			} else {
				ratio = "N/A";
			}
			
			String description;
			
			switch (type)
			{
				case NewIssues:
					
					if (event.stats.hits < input.minVolumeThreshold) {
						description = String.format("Nonsevere: volume %d < %d", 
								event.stats.hits, input.minVolumeThreshold);	
					} else {
						description = String.format("Nonsevere:rate %s < %.2f", 
								ratio, input.minErrorRateThreshold);
					}
					
					break;
					
				case SevereNewIssues:
					
					if (regResult.getCriticalNewEvents().containsKey(event.id)) {

						if (event.type.equals(UNCAUGHT_EXCEPTION)) {
							description = String.format("Severe: event is uncaught exception");					
						} else {
							description = String.format("Severe: event type " + 
								event.name + " is defined as a critical exception type");	
						}
					} else {
						description = String.format("Severe: (volume  %d > %s) AND (rate %s > %.2f)",
							event.stats.hits, input.minVolumeThreshold, ratio,  input.minErrorRateThreshold);
					}
					
					break;

					
				case Regressions:
					
					description = String.format("Nonsevere: (volume %d > %d) AND (rate %s > %.2f) AND (rate change from baseline %.2f > %.2f)",
							event.stats.hits, input.minVolumeThreshold,
							ratio, input.minErrorRateThreshold,
							getRateDelta(), input.regressionDelta);
					break;
					
				case SevereRegressions:
					
					description = String.format("Severe: (volume %d > %d) AND (rate %s > %.2f) AND (rate change from baseline %.2f > %.2f)",
							event.stats.hits, input.minVolumeThreshold,
							ratio, input.minErrorRateThreshold,
							getRateDelta(), input.criticalRegressionDelta);
					break;
					
				default: throw new IllegalStateException(String.valueOf(type));
			}
			
			return description + ". Thresholds are set in the Settings dashboard.";	
		}
		
		private double getRateDelta() {
			
			double baselineRate = (double) regression.getBaselineHits() 
				/ (double) regression.getBaselineInvocations();
				
			double activeRate = (double) event.stats.hits
					/ (double) event.stats.invocations;
					
			double result =  (activeRate / baselineRate) - 1;
			
			return result;
		}
		
		@Override
		public boolean equals(Object obj)
		{
			if (!super.equals(obj)) {
				return false;
			}
			
			RegressionData other = (RegressionData)obj;
			
			if (!Objects.equal(type, other.type)) {
				return false;
			}
			
			return true;
		}
		
		public String getText() {
			
			switch (type) {
				case NewIssues:
					return RegressionStringUtil.NEW_ISSUE;
				case SevereNewIssues:
					return RegressionStringUtil.SEVERE_NEW;
				case Regressions:
					return RegressionStringUtil.REGRESSION;
				case SevereRegressions:
					return RegressionStringUtil.SEVERE_REGRESSION;
			
				default:
					return null;
			}
		}
		
		@Override
		public String toString() {
			return String.valueOf(type) + " " + event.toString();
		}
	}
	
	protected static class RegressionFullRateFormatter  extends FieldFormatter {
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			RegressionData regData = (RegressionData)eventData;
			
			if (regData.regression != null) {
				return RegressionStringUtil.getRegressedEventRate(regData.regression, true);
			} else {
				return RegressionStringUtil.getEventRate(regData.event);
			}
		}
	}
	
	protected static class RegressionRateFormatter extends FieldFormatter {
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			RegressionData regData = (RegressionData)eventData;
			
			if (regData.regression != null) {
				return regData.getDelta();	
			} else {
				return RegressionStringUtil.getEventRate(regData.event);
			}
		}
	}
	
	protected class RegressionSeverityFormatter extends FieldFormatter {
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			RegressionReportSettings settings = getSettingsData(serviceId).regression_report;
			
			if (settings == null) {
				return Integer.valueOf(0);
			}
			
			RegressionData regData = (RegressionData)eventData;
			
			switch (regData.type) {
				
				case NewIssues:
				case Regressions:
					return Integer.valueOf(1);
					
				case SevereNewIssues:
				case SevereRegressions:
					return Integer.valueOf(2);
				default:
					break;
			}
			
			return Integer.valueOf(0);
		}
	}
	
	protected static class RegressionTimeRangeFormatter  extends FieldFormatter {
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			if (input.timeFilter == null) {
				return null;
			}
			
			String timeUnit = TimeUtil.getTimeRange(input.timeFilter); 
					
			return timeUnit;
		}
	}
	
	protected class RegressionFromFormatter  extends FieldFormatter {
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			if (input.timeFilter == null) {
				return null;
			}
			
			Pair<Object, Object> fromTo = getTimeFilterPair(timeSpan, input.timeFilter);
								
			return fromTo.getFirst();
		}
	}
	
	protected class RegressionToFormatter extends FieldFormatter
	{
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			if (input.timeFilter == null) {
				return null;
			}
			
			Pair<Object, Object> fromTo = getTimeFilterPair(timeSpan, input.timeFilter);
								
			return fromTo.getSecond();
		}
	}
	

	
	protected static class RegressionDescriptionFormatter extends FieldFormatter {
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			RegressionData regData = (RegressionData)eventData;
			String result = regData.getDescription();
			return result;
		}
		
		@Override
		protected Object formatValue(Object value, EventsInput input) {
			return value;
		}
	}
	
	private int expandBaselineTimespan(int baselineTimespanFactor, int minBaselineTimespan,
			RegressionWindow activeWindow) {
		
		int result;
		double factor = (double)minBaselineTimespan / (double)activeWindow.activeTimespan;
		
		if (factor > baselineTimespanFactor) {
			result = minBaselineTimespan;
		} else {
			result = activeWindow.activeTimespan * baselineTimespanFactor;
		}
		
		return result;
		
	}
	
	protected class RegressionLinkFormatter extends FieldFormatter {
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			RegressionData regData = (RegressionData)eventData;
			
			DateTime from = regData.regResult.getActiveWndowStart().minusMinutes(regData.input.baselineTimespan);
			DateTime to = DateTime.now();
			
			return EventLinkEncoder.encodeLink(apiClient, getSettingsData(serviceId), 
				serviceId, input, eventData.event, from, to);
		}
		
		@Override
		protected Object formatValue(Object value, EventsInput input) {
			return value;
		}
	}
	
	public RegressionFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	public RegressionFunction(ApiClient apiClient, Map<String, ServiceSettings> settingsMaps) {
		super(apiClient, settingsMaps);
	}
	
	private void sortRegressions(String serviceId, List<EventData> eventData) {	
		
		RegressionSettings regressionSettings = getSettingsData(serviceId).regression;
		
		int minThreshold = regressionSettings.error_min_volume_threshold;
		List<String> criticalExceptionList = new ArrayList<String>(regressionSettings.getCriticalExceptionTypes());
		
		eventData.sort(new Comparator<EventData>() {
			
			@Override
			public int compare(EventData o1, EventData o2) {
				
				RegressionData r1 = (RegressionData)o1;
				RegressionData r2 = (RegressionData)o2;
				
				int typeDelta = r1.type.ordinal() - r2.type.ordinal();
				
				if (typeDelta != 0) {
					return typeDelta;
				}
				
				if ((r1.type == RegressionType.SevereNewIssues) ||
					(r1.type == RegressionType.NewIssues)) {
							
					return compareEvents(o1.event, o2.event, 0, 0, 
						criticalExceptionList, minThreshold);
				}			
				
				if ((r1.type == RegressionType.SevereRegressions) ||
					(r1.type == RegressionType.Regressions)) {
					return r2.getDelta() - r1.getDelta();		
				}		
				
				throw new IllegalStateException(String.valueOf(r1.type));
			}
		});
	}
	
	private EventData findEventData(List<EventData> eventDatas, String Id) {
		
		for (EventData eventData : eventDatas) {
			if (eventData.event.id.equals(Id)) {
				return eventData;
			}
		}
		
		return null;
	}
	
	private List<EventData> getUniqueEventData(List<EventData> eventDatas, Map<String, EventResult> eventListMap) {
		
		List<EventData> result = new ArrayList<EventData>();
		
		for (EventData eventData : eventDatas) {
			
			RegressionData regData = (RegressionData)eventData;
			
			if (regData.regression != null) {
				result.add(regData);
			} else {
				
				boolean found = false;
						
				for (EventResult eventResult : eventListMap.values()) {
					
					if (eventResult.id.equals(regData.event.id)) {
						continue;
					}
					
					if (compareEvents(regData.event, eventResult)) {
						
						EventData matchingNewEvent = findEventData(eventDatas, eventResult.id);
						
						if (matchingNewEvent == null) {
							found = true;
							break;
						}
					}
				}
				
				if (!found) {
					result.add(regData);
				}
			}
		}
		
		return result;
	}
	
	public List<EventData> processRegression(String serviceId, EventFilterInput functionInput, RegressionInput input,
			RateRegression rateRegression, Map<String, EventResult> eventListMap,
			boolean includeNew, boolean includeRegressions) {
		
		List<EventData> result;
		List<EventData> eventDatas = processRegressionData(serviceId, input, rateRegression, includeNew, includeRegressions);
				
		if (functionInput.hasTransactions()) {
			result = eventDatas;
		} else {
			List<EventData> uniqueEventDatas = getUniqueEventData(eventDatas, eventListMap);
			result = doMergeSimilarEvents(input.serviceId, uniqueEventDatas);
		}
		
		return result;
	}
	
	private List<EventData> processRegressionData(String serviceId, RegressionInput input,
			RateRegression rateRegression, boolean includeNew, boolean includeRegressions) {
		
		List<EventData> result = new ArrayList<EventData>();
		
		if (includeNew) {
			
			for (EventResult event : rateRegression.getSortedCriticalNewEvents()) {
				result.add(new RegressionData(rateRegression, input, event, RegressionType.SevereNewIssues));
			}
			
			for (EventResult event : rateRegression.getSortedExceededNewEvents()) {
				result.add(new RegressionData(rateRegression, input, event, RegressionType.SevereNewIssues));
			}
			
			for (EventResult event : rateRegression.getSortedAllNewEvents()) {
				
				if (rateRegression.getExceededNewEvents().containsKey(event.id)) {
					continue;
				}
				
				if (rateRegression.getCriticalNewEvents().containsKey(event.id)) {
					continue;
				}
				
				result.add(new RegressionData(rateRegression, input, event, RegressionType.NewIssues));	
			}
		}
		
		if (includeRegressions) {
			
			for (RegressionResult regressionResult : rateRegression.getSortedCriticalRegressions()) {
				result.add(new RegressionData(rateRegression, input, regressionResult, RegressionType.SevereRegressions));
			}
			
			for (RegressionResult regressionResult : rateRegression.getSortedAllRegressions()) {
				
				if (rateRegression.getCriticalRegressions().containsKey(regressionResult.getEvent().id)) {
					continue;
				}
				
				result.add(new RegressionData(rateRegression, input, regressionResult, RegressionType.Regressions));
			}
		}
	
		sortRegressionDatas(serviceId, result);
		
		return result;
	}
	
	private void sortRegressionDatas(String serviceId, List<EventData> eventDatas) {
		
		RegressionSettings regressionSettings = getSettingsData(serviceId).regression;
		
		int minThreshold = regressionSettings.error_min_volume_threshold;
		List<String> criticalExceptionList = new ArrayList<String>(regressionSettings.getCriticalExceptionTypes());
		
		eventDatas.sort(new Comparator<EventData>() {

			@Override
			public int compare(EventData o1, EventData o2) {
				
				RegressionData r1 = (RegressionData)o1;
				RegressionData r2 = (RegressionData)o2;
				
				int typeDelta = Integer.compare(r2.type.ordinal(), r1.type.ordinal());

				if (typeDelta != 0) {
					return typeDelta;
				}
				
				int result = compareEvents(o1.event, o2.event, 
					0, 0, criticalExceptionList, minThreshold);
				
				return result;
			}
		});
		
	}
	
	@Override
	protected FieldFormatter getFormatter(String serviceId, String column) {
		
		if (column.equals(RegressionsInput.REG_DELTA)) {
			return new RegressionRateFormatter();
		}
		
		if (column.equals(RegressionsInput.REGRESSION)) {
			return new RegressionFullRateFormatter();
		}
		
		if (column.equals(RegressionsInput.SEVERITY)) {
			return new RegressionSeverityFormatter();
		}
		
		if (column.equals(EventsInput.LINK)) {
			return new RegressionLinkFormatter();
		}
		
		if (column.equals(ViewInput.TIME_RANGE)) {
			return new RegressionTimeRangeFormatter();
		}
		
		if (column.equals(ViewInput.FROM)) {
			return new RegressionFromFormatter();
		}
		
		if (column.equals(ViewInput.TO)) {
			return new RegressionToFormatter();
		}
		
		if (column.equals(RegressionsInput.REG_DESC)) {
			return new RegressionDescriptionFormatter();
		}
		
		return super.getFormatter(serviceId, column);
	}
	
	public static class RegressionOutput {	
		
		public static final RegressionOutput emptyOutput = new RegressionOutput(true);
		
		public boolean empty;
		
		public RegressionInput regressionInput;
		public RegressionWindow regressionWindow;
		public RateRegression rateRegression;
		public Graph baseVolumeGraph;
		public Graph activeVolumeGraph;
		public Map<String, EventResult> eventListMap;
		public List<EventData> eventDatas;
		
		protected double score;
		protected long volume;
		
		protected int severeNewIssues;
		protected int newIssues;
		
		protected int criticalRegressions;
		protected int regressions;
		
		public RegressionOutput(boolean empty) {
			this.empty = empty;
		}
		
		public double getStat(RegressionType type) {
			
			switch (type) {	
				case NewIssues:
					return newIssues;
				
				case SevereNewIssues:
					return severeNewIssues;
				
				case Regressions:
					return regressions;
				
				case SevereRegressions:
					return criticalRegressions;
					
				default:
					return 0;
			}
		}
	}
	
	private RegressionSettings getRegressionSettings(String serviceId) {
		
		RegressionSettings regressionSettings = getSettingsData(serviceId).regression;
		
		if (regressionSettings == null) {
			throw new IllegalStateException("Missing regression settings for " + serviceId);
		}
		
		return regressionSettings;
	}
	
	public Pair<RegressionInput, RegressionWindow> getRegressionInput(String serviceId, String viewId,
			EventFilterInput input,
			Pair<DateTime, DateTime> timeSpan, boolean newOnly) {
		return getRegressionInput(serviceId, viewId, input, null, timeSpan, newOnly);
	}
	
	public Pair<RegressionInput, RegressionWindow> getRegressionInput(String serviceId,
			EventFilterInput input,
			Pair<DateTime, DateTime> timeSpan, boolean newOnly) {
		
		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null) {
			return null;
		}
		
		return getRegressionInput(serviceId, viewId, input, null, timeSpan, newOnly);
	}
		
	
	public Pair<RegressionInput, RegressionWindow> getRegressionInput(String serviceId, String viewId,
		EventFilterInput input, RegressionWindow window,
		Pair<DateTime, DateTime> timeSpan, boolean newOnly) {
		
		RegressionSettings regressionSettings = getRegressionSettings(serviceId);
		
		RegressionInput regressionInput = new RegressionInput();
		
		regressionInput.serviceId = serviceId;
		regressionInput.viewId = viewId;
		regressionInput.deployments = input.getDeployments(serviceId, apiClient);
		
		regressionInput.activeTimespan = (int)TimeUnit.MILLISECONDS
				.toMinutes(timeSpan.getSecond().getMillis() - timeSpan.getFirst().getMillis());
		
		if ((CollectionUtil.safeIsEmpty(regressionInput.deployments))) {
			regressionInput.activeWindowStart = timeSpan.getFirst();
		}
		
		regressionInput.baselineTimespan = regressionSettings.min_baseline_timespan;
		
		RegressionWindow regressionWindow;

		if (window == null) {
			regressionWindow = ApiCache.getRegressionWindow(apiClient, regressionInput);
		} else {
			regressionWindow = window;
		}
		
		if ((!CollectionUtil.safeIsEmpty(regressionInput.deployments)) 
		&& (!regressionWindow.deploymentFound)) {
			return null;
		}
		
		int expandedBaselineTimespan = expandBaselineTimespan(regressionSettings.baseline_timespan_factor,
				regressionSettings.min_baseline_timespan,
				regressionWindow);
		
		regressionInput.activeWindowStart = regressionWindow.activeWindowStart;
		regressionInput.activeTimespan = regressionWindow.activeTimespan;
		regressionInput.baselineTimespan = expandedBaselineTimespan;
		
		regressionInput.applictations = input.getApplications(apiClient,
			getSettingsData(serviceId), serviceId, true, false);
		
		regressionInput.servers = input.getServers(serviceId);
		
		Collection<String> criticalExceptionTypes = regressionSettings.getCriticalExceptionTypes();
		
		regressionInput.criticalExceptionTypes = criticalExceptionTypes;
		
		regressionInput.minVolumeThreshold = regressionSettings.error_min_volume_threshold;
		regressionInput.minErrorRateThreshold = regressionSettings.error_min_rate_threshold;
		
		if (!newOnly) {
			regressionInput.regressionDelta = regressionSettings.error_regression_delta;
			regressionInput.criticalRegressionDelta = regressionSettings.error_critical_regression_delta;
			regressionInput.applySeasonality = regressionSettings.apply_seasonality;
		}
		
		return Pair.of(regressionInput, regressionWindow);
		
	}
	
	private Pair<Graph, Graph> getRegressionGraphs(String serviceId, String viewId,
			RegressionInput regressionInput, RegressionWindow regressionWindow,
			BaseEventVolumeInput input, boolean newOnly) {
		
		EventFilterInput baselineInput;
		
		if (input.hasDeployments()) {
			Gson gson = new Gson();
			baselineInput = gson.fromJson(gson.toJson(input), input.getClass());
			
			//deployments by definition nature do not have their own baseline - 
			//they are compared against the general baseline (all prev deps)
			baselineInput.deployments = null;
		} else {
			baselineInput = input;
		}
				
		Collection<GraphSliceTask> baselineGraphTasks;
		
		DateTime baselineStart = regressionWindow.activeWindowStart.minusMinutes(regressionInput.baselineTimespan);
		DateTime baselineEnd = regressionWindow.activeWindowStart;
		
		if (!newOnly) {
			baselineGraphTasks = getGraphTasks(serviceId, viewId, baselineInput, 
				VolumeType.all, baselineStart, baselineEnd,
				regressionInput.baselineTimespan, regressionWindow.activeTimespan, false);
		} else {
			baselineGraphTasks = null;
		}
		
		int graphActiveTimespan;
		
		if (input.hasDeployments()) {
			graphActiveTimespan = regressionWindow.activeTimespan;
		} else {
			graphActiveTimespan = 0;
		}
		
		DateTime activeStart = regressionWindow.activeWindowStart;
		DateTime activeEnd = regressionWindow.activeWindowStart.plusMinutes(regressionWindow.activeTimespan);
		
		Collection<GraphSliceTask> activeGraphTasks = getGraphTasks(serviceId, viewId, 
			input, VolumeType.all, activeStart, activeEnd, 0, graphActiveTimespan, false);
		
		List<GraphSliceTask> graphTasks = new ArrayList<GraphSliceTask>(); 
		
		if (baselineGraphTasks != null) {
			graphTasks.addAll(baselineGraphTasks);
		}
		
		graphTasks.addAll(activeGraphTasks);
		
		Collection<GraphSliceTaskResult> graphSliceTaskResults = executeGraphTasks(graphTasks, false);
		
		List<GraphSliceTaskResult> baseLineGraphResults = new ArrayList<GraphSliceTaskResult>();
		List<GraphSliceTaskResult> activeGraphResults = new ArrayList<GraphSliceTaskResult>();
	
		for (GraphSliceTaskResult graphSliceTaskResult : graphSliceTaskResults) {
			
			if (CollectionUtil.safeContains(baselineGraphTasks, graphSliceTaskResult.task)) {
				baseLineGraphResults.add(graphSliceTaskResult);
			}
			
			if (activeGraphTasks.contains(graphSliceTaskResult.task)) {
				activeGraphResults.add(graphSliceTaskResult);
			}
		}
		
		Graph baselineGraph = mergeGraphs(baseLineGraphResults);
		Graph activeWindowGraph = mergeGraphs(activeGraphResults);		
		
		GraphResult activeGraphResult = new GraphResult();
		activeGraphResult.graphs = Collections.singletonList(activeWindowGraph);	
	
		return Pair.of(baselineGraph, activeWindowGraph);	
	}
	
	protected RegressionOutput createRegressionOutput(String serviceId, 
			EventFilterInput input, RegressionInput regressionInput, RegressionWindow regressionWindow,
			RateRegression rateRegression, Map<String, EventResult> eventListMap,
			Graph baseVolumeGraph, Graph activeVolumeGraph, long volume,
			boolean allowEmpty) {
		
		RegressionOutput result = new RegressionOutput(false);
		
		result.regressionInput = regressionInput;
		result.regressionWindow = regressionWindow;
		result.rateRegression = rateRegression;
		result.baseVolumeGraph = baseVolumeGraph;
		result.activeVolumeGraph = activeVolumeGraph;
		result.eventListMap = eventListMap;
		result.volume = volume;
		
		if ((regressionInput != null) && (rateRegression != null)) {
			
			result.eventDatas = processRegression(serviceId, input, regressionInput, 
				rateRegression, eventListMap, true, true);
			
			for (EventData eventData : result.eventDatas) {
				
				RegressionData regData = (RegressionData)eventData;
				
				if ((!allowEmpty) && (regData.event.stats.hits == 0))  {
					continue;
				}
				
				switch (regData.type) {
					case NewIssues:
						result.newIssues++;
						break;
					case SevereNewIssues:
						result.severeNewIssues++;
						break;
					case Regressions:
						result.regressions++;
						break;
					case SevereRegressions:
						result.criticalRegressions++;
						break;
					
					default:
				}
			}
		}
		
		return result;
	}
	
	public RegressionOutput executeRegression(String serviceId, 
			BaseEventVolumeInput input, boolean newOnly) {
		
		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null) {
			return RegressionOutput.emptyOutput;
		}
		
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(input.timeFilter);
		
		Pair<RegressionInput, RegressionWindow> regressionInputs =
				getRegressionInput(serviceId, viewId, input, timespan, newOnly);
		
		if (regressionInputs == null) {
			return RegressionOutput.emptyOutput;
		}
		
		RegressionInput regressionInput = regressionInputs.getFirst();
		RegressionWindow regressionWindow = regressionInputs.getSecond();
		
		DateTime from = regressionWindow.activeWindowStart;
		DateTime to = regressionWindow.activeWindowStart.plusMinutes(regressionInput.activeTimespan);
		
		Map<String, EventResult> eventListMap = getEventMap(serviceId, input,
			from, to, null);
			
		if (eventListMap == null) {
			return RegressionOutput.emptyOutput;
		}	
		
		Pair<Graph, Graph> regressionGraphs = getRegressionGraphs(serviceId, 
			viewId, regressionInput, regressionWindow, input, newOnly);
			
		Graph baselineGraph = regressionGraphs.getFirst();
		Graph activeWindowGraph = regressionGraphs.getSecond();
		
		if ((activeWindowGraph == null) ||(activeWindowGraph.points == null)) {
			return RegressionOutput.emptyOutput;
		}
		
		if ((!newOnly) && (((baselineGraph == null) || (baselineGraph.points == null)))) {
			return RegressionOutput.emptyOutput;
		}
		
		Pair<Map<String, EventResult>, Long> filteredResult = filterEvents(serviceId, 
			timespan, input, eventListMap.values());
		
		Map<String, EventResult> filteredMap = filteredResult.getFirst();
		
		long volume = applyGraphToEvents(filteredMap, activeWindowGraph, timespan);
		
		regressionInput.events = filteredMap.values();
		regressionInput.baselineGraph = baselineGraph;
				
		RegressionOutput result = executeRegression(serviceId, input, regressionInput, 
			regressionWindow, eventListMap, volume, baselineGraph, activeWindowGraph, false);
	
		return result;
	}	
	
	/**
	 * @param allowEmpty - skipped for now 
	 */
	public RegressionOutput executeRegression(String serviceId, BaseEventVolumeInput input, 
			RegressionInput regressionInput, RegressionWindow regressionWindow, 
			Map<String, EventResult> eventListMap, long volume,
			Graph baselineGraph, Graph activeWindowGraph, boolean allowEmpty) {
		
		regressionInput.validate();
		
		RateRegression rateRegression = RegressionUtil.calculateRateRegressions(apiClient, regressionInput, null,
				false);
		
		RegressionOutput result = createRegressionOutput(serviceId,
				input, regressionInput, regressionWindow,
				rateRegression, eventListMap,
				baselineGraph, activeWindowGraph, volume, true);//allowEmpty);
		
		return result;
	}
	
	private Map<RegressionType, List<EventData>> getRegressionMap(List<EventData> eventDatas) {
		
		Map<RegressionType, List<EventData>> result = new HashMap<RegressionType, List<EventData>>();
		
		for (EventData eventData : eventDatas) {
			RegressionData regData = (RegressionData)eventData;
			List<EventData> typeEvents = result.get(regData.type);
			
			if (typeEvents == null) {
				typeEvents = new ArrayList<EventData>();
				result.put(regData.type, typeEvents);
			}
			
			typeEvents.add(regData);
		}
			
		return result;
	}
	
	private EventData mergeRegressionsOfType(List<EventData> eventDatas) {
		
		RegressionData first = (RegressionData)(eventDatas.get(0));
		List<EventData> merged = super.mergeEventDatas(eventDatas);
		
		RegressionData result = new RegressionData(first.regResult, first.input, merged.get(0).event, first.type);
		
		long baselineHits = 0;
		long baselineInvocations = 0;
			
		for (EventData eventData : eventDatas) {	
			
			RegressionData regressionData = (RegressionData)eventData;
				
			if (regressionData.regression != null) {
				baselineHits += regressionData.regression.getBaselineHits();
				baselineInvocations += regressionData.regression.getBaselineInvocations();
			}
				
			result.mergedIds.add(eventData.event.id);
				
			if (eventData.event.similar_event_ids != null) {
				result.mergedIds.addAll(eventData.event.similar_event_ids);
			}
		}
				
		if (first.regression != null) {
			result.regression = RegressionResult.of(result.event, baselineHits, baselineInvocations);
		}
		
		return result;
	}
	
	@Override
	protected List<EventData> mergeEventDatas(List<EventData> eventDatas) {
		
		List<EventData> result = new ArrayList<EventData>();
		Map<RegressionType, List<EventData>> regressionMap = getRegressionMap(eventDatas);
		
		for (List<EventData> typeEvents : regressionMap.values()) {
			result.add(mergeRegressionsOfType(typeEvents));
		}
		
		return result;
	}
	
	private List<EventData> doMergeSimilarEvents(String serviceId, List<EventData> eventDatas) {
		
		List<EventData> result = new ArrayList<EventData>(super.mergeSimilarEvents(serviceId, eventDatas));
		sortRegressions(serviceId, result);
		
		return result;
	}
	
	@Override
	protected void sortEventDatas(String serviceId, List<EventData> eventDatas) {
		//use regression sorting instead of normal event ranking
	}
	
	@Override
	protected List<EventData> mergeSimilarEvents(String serviceId, List<EventData> eventDatas) {
		return eventDatas;
	}
		
	public RegressionOutput runRegression(String serviceId, EventFilterInput regInput, boolean newOnly) {
		RegressionOutput regressionOutput = ApiCache.getRegressionOutput(apiClient, 
			serviceId, regInput, this, newOnly, true);
		return regressionOutput;
	}
	
	@Override
	protected List<EventData> getEventData(String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan) {
		
		RegressionsInput regInput = (RegressionsInput)input;
				
		RegressionOutput regressionOutput = runRegression(serviceId, regInput, regInput.newOnly());
		
		if ((regressionOutput == null) || (regressionOutput.rateRegression == null) ||
			(regressionOutput.regressionInput == null)) {
			return Collections.emptyList();
		}
		
		List<EventData> result;
		
		if (regInput.regressionTypes != null) {
			
			Collection<RegressionType> types = regInput.getRegressionTypes();
			
			result = new ArrayList<EventData>(regressionOutput.eventDatas.size());
			
			for (EventData eventData : regressionOutput.eventDatas) {
				RegressionData regData = (RegressionData)eventData;
				
				if (types.contains(regData.type)) {
					result.add(regData);
				}
			}
		} else {
			result = regressionOutput.eventDatas;
		}
		
		return result;
	}
	
	public static String getNewIssuesDesc(RegressionOutput regressionOutput, int maxIssuesSize) {
		
		StringBuilder result = new StringBuilder();
		
		int size = 0;
		int issuesSize = regressionOutput.newIssues + regressionOutput.severeNewIssues;
		
		if (!CollectionUtil.safeIsEmpty(regressionOutput.eventDatas)) {
			for (EventData eventData : regressionOutput.eventDatas) {
				
				if (!(eventData instanceof RegressionData)) {
					continue;
				}
				
				RegressionData regressionData = (RegressionData)eventData;
				
				if (regressionData.regression != null) {
					continue;
				}
				
				EventResult newEvent = regressionData.event;
				
				if (newEvent.error_location != null) {
					result.append(newEvent.name);
					result.append(" in ");
					result.append(getSimpleClassName(newEvent.error_location.class_name));
					size++;
				} else {
					continue;
				}
					
				if (size == maxIssuesSize) {
					break;
				} else {
					if (size < issuesSize) {
						result.append(", ");
					}
				}
			}
		}
		
		int remaining = issuesSize - size;
		
		if (remaining > 0) {
			result.append("\nand ");
			result.append(remaining);
			result.append(" more");
		}
		
		return result.toString();
	}
	
	public static String getRegressionsDesc(RegressionOutput regressionOutput, int maxItems) {
		
		StringBuilder result = new StringBuilder();
		
		int size = 0;
		int regressionsSize = regressionOutput.regressions + regressionOutput.criticalRegressions;
		
		if (!CollectionUtil.safeIsEmpty(regressionOutput.eventDatas)) {
			
			for (EventData eventData : regressionOutput.eventDatas) {
				
				if (!(eventData instanceof RegressionData)) {
					continue;
				}
				
				RegressionData regressionData = (RegressionData)eventData;
				
				if (regressionData.regression == null) {
					continue;
				}
				
				double baseRate = (double) regressionData.regression.getBaselineHits() /
					(double)  regressionData.regression.getBaselineInvocations();
				
				double activeRate = (double) regressionData.event.stats.hits /
					(double) regressionData.event.stats.invocations;
	
				int delta = (int)((activeRate - baseRate) * 100);
				
				if (delta < 1000) {
					result.append("+"); 
					result.append(delta);
				} else {
					result.append(">1000"); 
				}
				
				result.append("% "); 
	
				result.append(regressionData.event.name);
				
				if (regressionData.event.error_location != null) {
					result.append(" in ");
					result.append(getSimpleClassName(regressionData.event.error_location.class_name));
				}
							
				size++;
				
				if (size == maxItems) {
					break;
				} else {
					if (size < regressionsSize) {
						result.append(", ");
					}
				}
			}
		}
		
		int remaining = regressionsSize - size;
		
		if (remaining > 0) {
			result.append("\nand ");
			result.append(remaining);
			result.append(" more");
		}
		
		return result.toString();
	}
		
	private double getServiceSingleStatCount(String serviceId, RegressionsInput input) {
		RegressionOutput regressionOutput = runRegression(serviceId, input, input.newOnly());
		
		if ((regressionOutput == null) || (regressionOutput.empty)) {
			return 0;
		}
		
		double result = 0;
		Collection<RegressionType> regressionTypes = input.getRegressionTypes();
		
		for (RegressionType regressionType : regressionTypes) {
			result += regressionOutput.getStat(regressionType);
		}
		
		return result;
	}
	
	private double getSingleStatCount(Collection<String> serviceIds, RegressionsInput input) {
		
		double result = 0;
		
		for (String serviceId : serviceIds) {
			result += getServiceSingleStatCount(serviceId, input);
		}
		
		return result;
	}
	
	private List<Series> processSingleStatCount(RegressionsInput input) {
		
		Collection<String> serviceIds = getServiceIds(input);
		
		if (CollectionUtil.safeIsEmpty(serviceIds)) {
			return Collections.emptyList();
		}
		
		Collection<RegressionType> regressionTypes = input.getRegressionTypes();

		if (regressionTypes == null) {
			return Collections.emptyList();
		}
		
		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		
		Object singleStatText;
		double singleStatValue = getSingleStatCount(serviceIds, input);
			
		if (input.singleStatFormat != null) {
			
			if (singleStatValue > 0) {
				singleStatText = String.format(input.singleStatFormat, String.valueOf((int)singleStatValue));
			} else {
				singleStatText = EMPTY_POSTFIX;
			}
		}
		else {
			singleStatText = Integer.valueOf((int)singleStatValue);
		}
		
		return createSingleStatSeries(timeSpan, singleStatText);
	}
	
	private List<Series> processSingleStatVolume(RegressionsInput input, boolean textValue) {
		
		Collection<String> serviceIds = getServiceIds(input);
		
		if (CollectionUtil.safeIsEmpty(serviceIds)) {
			return Collections.emptyList();
		}
		
		Collection<RegressionType> regressionTypes = input.getRegressionTypes();

		if (regressionTypes == null) {
			return Collections.emptyList();
		}
		
		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		
		long singleStatValue = 0;
		
		for (String serviceId : serviceIds) {
			
			RegressionOutput regressionOutput = runRegression(serviceId, input, input.newOnly());
			
			if ((regressionOutput == null) || (regressionOutput.empty)) {
				continue;
			}
						
			for (EventData eventData : regressionOutput.eventDatas) {
				
				RegressionData regData = (RegressionData)eventData;
				
				if (!regressionTypes.contains(regData.type)) {
					continue;
				}
				
				singleStatValue += regData.event.stats.hits;
			}
			
			singleStatValue += getServiceSingleStatCount(serviceId, input);
		}
			
		Object value;
		
		if (textValue) {
			value = formatLongValue(singleStatValue);
		} else {
			value = 	singleStatValue;
		}
			
		return createSingleStatSeries(timeSpan, value);
	}
	
	private List<Series> processSingleStatDesc(RegressionsInput input) {
		
		Collection<String> serviceIds = getServiceIds(input);
		
		if (CollectionUtil.safeIsEmpty(serviceIds)) {
			return Collections.emptyList();
		}
		
		Collection<RegressionType> regressionTypes = input.getRegressionTypes();

		if (regressionTypes == null) {
			return Collections.emptyList();
		}
		
		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		
		StringBuilder result = new StringBuilder();

		for (String serviceId : serviceIds) {
			
			RegressionOutput regressionOutput = runRegression(serviceId, input, input.newOnly());

			String value;
			
			if ((regressionTypes.contains(RegressionType.NewIssues)) 
			|| (regressionTypes.contains(RegressionType.SevereNewIssues))) {
				value = getNewIssuesDesc(regressionOutput, RegressionsInput.MAX_TOOLTIP_ITEMS);
			} else {
				value = getRegressionsDesc(regressionOutput, RegressionsInput.MAX_TOOLTIP_ITEMS);

			}
				
			if (serviceIds.size() > 1) {
				result.append(serviceId);
				result.append(" = ");
			}
				
			result.append(value);
			
			if (serviceIds.size() > 1) {
				result.append(". ");
			}
		}
			
		return createSingleStatSeries(timeSpan, result.toString());
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof RegressionsInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		RegressionsInput regInput = (RegressionsInput)getInput((ViewInput)functionInput);
		
		if (regInput.render == null) {
			throw new IllegalStateException("Missing render mode");
		}
		
		switch (regInput.render) {
			
			case Grid:
				return super.process(functionInput);
			
			case Graph:
				throw new IllegalStateException("Graph not supported. Use RegressionGraph");
				
			case SingleStat:
				return processSingleStatCount(regInput);
				
			case SingleStatDesc:
				return processSingleStatDesc(regInput);
				
			case SingleStatCount:
				return processSingleStatCount(regInput);
				
			case SingleStatVolume:
				return processSingleStatVolume(regInput, false);
				
			case SingleStatVolumeText:
				return processSingleStatVolume(regInput, true);
			
			default: 
				throw new IllegalStateException(String.valueOf(regInput.render));
		}
	}
}
