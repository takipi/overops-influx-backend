package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.api.core.url.UrlClient.Response;
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
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.input.RegressionReportSettings;
import com.takipi.integrations.grafana.settings.input.RegressionSettings;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.EventLinkEncoder;
import com.takipi.integrations.grafana.util.TimeUtil;

public class RegressionFunction extends EventsFunction
{	
	public static class Factory implements FunctionFactory
	{
		
		@Override
		public GrafanaFunction create(ApiClient apiClient)
		{
			return new RegressionFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass()
		{
			return RegressionsInput.class;
		}
		
		@Override
		public String getName()
		{
			return "regressions";
		}
	}
	
	protected class RegressionData extends EventData
	{
		
		protected RegressionType type;
		protected RegressionResult regression;
		protected RateRegression regResult;
		protected RegressionInput input;
		protected Set<String> mergedIds;
		
		protected RegressionData(RateRegression regResult, RegressionInput input,
				EventResult event, RegressionType type)
		{
			super(event);
			this.type = type;
			this.regResult = regResult;
			this.input = input;
			this.regression = null;
			this.mergedIds = new HashSet<String>();
		}
			
		protected RegressionData(RateRegression regResult, RegressionInput input,
				RegressionResult regression, RegressionType type)
		{
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
			if (!super.equals(obj))
			{
				return false;
			}
			
			RegressionData other = (RegressionData)obj;
			
			if (!Objects.equal(type, other.type))
			{
				return false;
			}
			
			return true;
		}
		
		public String getText()
		{
			
			switch (type)
			{
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
		public String toString()
		{
			return String.valueOf(type) + " " + event.toString();
		}
	}
	
	protected static class RegressionFullRateFormatter  extends FieldFormatter
	{
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan)
		{
			
			RegressionData regData = (RegressionData)eventData;
			
			if (regData.regression != null)
			{
				return RegressionStringUtil.getRegressedEventRate(regData.regression, true);
			}
			else
			{
				return RegressionStringUtil.getEventRate(regData.event);
			}
		}
	}
	
	protected static class RegressionRateFormatter extends FieldFormatter
	{
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan)
		{
			
			RegressionData regData = (RegressionData)eventData;
			
			if (regData.regression != null)
			{
				return regData.getDelta();	
			}
			else
			{
				return RegressionStringUtil.getEventRate(regData.event);
			}
		}
	}
	
	protected class RegressionSeverityFormatter extends FieldFormatter
	{
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan)
		{
			
			RegressionReportSettings settings = GrafanaSettings.getData(apiClient, serviceId).regression_report;
			
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
	
	protected static class RegressionTimeRangeFormatter  extends FieldFormatter
	{
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan)
		{
			if (input.timeFilter == null) {
				return null;
			}
			
			String timeUnit = TimeUtil.getTimeRange(input.timeFilter); 
					
			return timeUnit;
		}
	}
	
	protected class RegressionFromFormatter  extends FieldFormatter
	{
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan)
		{
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
				Pair<DateTime, DateTime> timeSpan)
		{
			if (input.timeFilter == null) {
				return null;
			}
			
			Pair<Object, Object> fromTo = getTimeFilterPair(timeSpan, input.timeFilter);
								
			return fromTo.getSecond();
		}
	}
	

	
	protected static class RegressionDescriptionFormatter extends FieldFormatter
	{
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan)
		{
			RegressionData regData = (RegressionData)eventData;
			String result = regData.getDescription();
			return result;
		}
		
		@Override
		protected Object formatValue(Object value, EventsInput input)
		{
			return value;
		}
	}
	
	private int expandBaselineTimespan(int baselineTimespanFactor, int minBaselineTimespan,
			RegressionWindow activeWindow)
	{
		
		int result;
		double factor = (double)minBaselineTimespan / (double)activeWindow.activeTimespan;
		
		if (factor > baselineTimespanFactor)
		{
			result = minBaselineTimespan;
		}
		else
		{
			result = activeWindow.activeTimespan * baselineTimespanFactor;
		}
		
		return result;
		
	}
	
	protected class RegressionLinkFormatter extends FieldFormatter
	{
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan)
		{
			
			RegressionData regData = (RegressionData)eventData;
			
			DateTime from = regData.regResult.getActiveWndowStart().minusMinutes(regData.input.baselineTimespan);
			DateTime to = DateTime.now();
			
			return EventLinkEncoder.encodeLink(apiClient, serviceId, input, eventData.event, from, to);
		}
		
		@Override
		protected Object formatValue(Object value, EventsInput input)
		{
			return value;
		}
	}
	
	public RegressionFunction(ApiClient apiClient)
	{
		super(apiClient);
	}
	
	private void sortRegressions(List<EventData> eventData)
	{		
		eventData.sort(new Comparator<EventData>()
		{
			
			@Override
			public int compare(EventData o1, EventData o2)
			{
				RegressionData r1 = (RegressionData)o1;
				RegressionData r2 = (RegressionData)o2;
				
				int typeDelta = r1.type.ordinal() - r2.type.ordinal();
				
				if (typeDelta != 0) {
					return typeDelta;
				}
				
				if ((r1.type == RegressionType.SevereNewIssues) ||
					(r1.type == RegressionType.NewIssues)) {
							
					if (r2.event.stats.hits - r1.event.stats.hits > 0) {
						return -1;
					}
					
					if (r1.event.stats.hits - r2.event.stats.hits > 0) {
						return 1;
					}
					
					if (r1.event.stats.hits - r2.event.stats.hits == 0) {
						return 0;
					}
				}			
				
				if ((r1.type == RegressionType.SevereRegressions) ||
					(r1.type == RegressionType.Regressions)) {
					return r2.getDelta() - r1.getDelta();		
				}		
				
				throw new IllegalStateException();
			}
		});
	}
	
	public List<EventData> processRegression(EventFilterInput functionInput, RegressionInput input,
			RateRegression rateRegression, boolean includeNew, boolean includeRegressions)
	{
		List<EventData> result;
		List<EventData> eventDatas = processRegressionData(input, rateRegression, includeNew, includeRegressions);
				
		if (functionInput.hasTransactions()) {
			result = eventDatas;
		} else {
			result = doMergeSimilarEvents(input.serviceId, eventDatas);
		}
		
		return result;
	}
	
	private List<EventData> processRegressionData(RegressionInput input,
			RateRegression rateRegression, boolean includeNew, boolean includeRegressions)
	{
		
		List<EventData> result = new ArrayList<EventData>();
		
		if (includeNew)
		{
			
			for (EventResult event : rateRegression.getSortedCriticalNewEvents())
			{
				result.add(new RegressionData(rateRegression, input, event, RegressionType.SevereNewIssues));
			}
			
			for (EventResult event : rateRegression.getSortedExceededNewEvents())
			{
				result.add(new RegressionData(rateRegression, input, event, RegressionType.SevereNewIssues));
			}
			
			for (EventResult event : rateRegression.getSortedAllNewEvents())
			{
				
				if (rateRegression.getExceededNewEvents().containsKey(event.id))
				{
					continue;
				}
				
				if (rateRegression.getCriticalNewEvents().containsKey(event.id))
				{
					continue;
				}
				
				result.add(new RegressionData(rateRegression, input, event, RegressionType.NewIssues));
				
			}
		}
		
		if (includeRegressions) {
			for (RegressionResult regressionResult : rateRegression.getSortedCriticalRegressions())
			{
				result.add(new RegressionData(rateRegression, input, regressionResult, RegressionType.SevereRegressions));
			}
			
			for (RegressionResult regressionResult : rateRegression.getSortedAllRegressions())
			{
				
				if (rateRegression.getCriticalRegressions().containsKey(regressionResult.getEvent().id))
				{
					continue;
				}
				
				result.add(new RegressionData(rateRegression, input, regressionResult, RegressionType.Regressions));
			}
		}
		
		return result;
	}
	
	@Override
	protected FieldFormatter getFormatter(String serviceId, String column)
	{
		
		if (column.equals(RegressionsInput.REG_DELTA))
		{
			return new RegressionRateFormatter();
		}
		
		if (column.equals(RegressionsInput.REGRESSION))
		{
			return new RegressionFullRateFormatter();
		}
		
		if (column.equals(RegressionsInput.SEVERITY))
		{
			return new RegressionSeverityFormatter();
		}
		
		if (column.equals(EventsInput.LINK))
		{
			return new RegressionLinkFormatter();
		}
		
		if (column.equals(ViewInput.TIME_RANGE))
		{
			return new RegressionTimeRangeFormatter();
		}
		
		if (column.equals(ViewInput.FROM))
		{
			return new RegressionFromFormatter();
		}
		
		if (column.equals(ViewInput.TO))
		{
			return new RegressionToFormatter();
		}
		
		if (column.equals(RegressionsInput.REG_DESC))
		{
			return new RegressionDescriptionFormatter();
		}
		
		return super.getFormatter(serviceId, column);
	}
	
	public static class RegressionOutput
	{	
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
	
	private RegressionSettings getRegressionSettings(String serviceId)
	{
		
		RegressionSettings regressionSettings = GrafanaSettings.getData(apiClient, serviceId).regression;
		
		if (regressionSettings == null)
		{
			throw new IllegalStateException("Missing regression settings for " + serviceId);
		}
		
		return regressionSettings;
	}
	
	public Pair<RegressionInput, RegressionWindow> getRegressionInput(String serviceId, String viewId,
			EventFilterInput input,
			Pair<DateTime, DateTime> timeSpan, boolean newOnly)
	{
		
		RegressionSettings regressionSettings = getRegressionSettings(serviceId);
		
		RegressionInput regressionInput = new RegressionInput();
		
		regressionInput.serviceId = serviceId;
		regressionInput.viewId = viewId;
		regressionInput.deployments = input.getDeployments(serviceId);
		
		regressionInput.activeTimespan = (int)TimeUnit.MILLISECONDS
				.toMinutes(timeSpan.getSecond().getMillis() - timeSpan.getFirst().getMillis());
		
		regressionInput.baselineTimespan = regressionSettings.min_baseline_timespan;
		
		RegressionWindow regressionWindow = ApiCache.getRegressionWindow(apiClient, regressionInput);
		
		if ((!CollectionUtil.safeIsEmpty(regressionInput.deployments)) && (!regressionWindow.deploymentFound))
		{
			return null;
		}
		
		int expandedBaselineTimespan = expandBaselineTimespan(regressionSettings.baseline_timespan_factor,
				regressionSettings.min_baseline_timespan,
				regressionWindow);
		
		regressionInput.activeWindowStart = regressionWindow.activeWindowStart;
		regressionInput.activeTimespan = regressionWindow.activeTimespan;
		regressionInput.baselineTimespan = expandedBaselineTimespan;
		
		regressionInput.applictations = input.getApplications(apiClient, serviceId);
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
			BaseEventVolumeInput input, boolean newOnly)
	{
		
		int ratioBaselinePoints = (regressionInput.baselineTimespan / regressionWindow.activeTimespan) * 2;
		int baselineDays = (int)TimeUnit.MINUTES.toDays(regressionInput.baselineTimespan);
		
		int baselinePoints = Math.max(Math.min(ratioBaselinePoints, baselineDays / 3), input.pointsWanted);
		
		if (baselinePoints <= 0)
		{
			throw new IllegalStateException(
					"Negative points for dep " +	Arrays.toString(regressionInput.deployments.toArray()) + " " +
											ratioBaselinePoints + " " + baselineDays + "  " +
											regressionWindow.activeWindowStart);
		}
		
		EventFilterInput baselineInput;
		
		if (input.hasDeployments()) {
			Gson gson = new Gson();
			baselineInput = gson.fromJson(gson.toJson(input), input.getClass());
			baselineInput.deployments = null;
		} else {
			baselineInput = input;
		}
				
		/*
		Graph baselineGraph = getEventsGraph(serviceId, viewId, baselinePoints, 
				input, VolumeType.all,
				regressionWindow.activeWindowStart.minusMinutes(regressionInput.baselineTimespan),
				regressionWindow.activeWindowStart, 0, regressionInput.baselineTimespan);
	
		Graph activeWindowGraph = getEventsGraph(serviceId, viewId, input.pointsWanted, input,
				VolumeType.all, regressionWindow.activeWindowStart, DateTime.now(), regressionWindow.activeTimespan, 0);
		*/
		
		//This section needs to be refactored into its own blocking / synch cache loader 
		
		Collection<GraphSliceTask> baselineGraphTasks;
		
		if (!newOnly) {
			baselineGraphTasks = getGraphTasks(serviceId, viewId, baselinePoints, baselineInput, 
				VolumeType.all, regressionWindow.activeWindowStart.minusMinutes(regressionInput.baselineTimespan),
				regressionWindow.activeWindowStart, regressionInput.baselineTimespan, regressionWindow.activeTimespan, false);
		} else {
			baselineGraphTasks = null;
		}
		
		int graphActiveTimespan;
		
		if (input.hasDeployments()) {
			graphActiveTimespan = regressionWindow.activeTimespan;
		} else {
			graphActiveTimespan = 0;
		}
		
		Collection<GraphSliceTask> activeGraphTasks = getGraphTasks(serviceId, viewId, input.pointsWanted, 
			input, VolumeType.all, regressionWindow.activeWindowStart, DateTime.now(), 0, graphActiveTimespan, false);
		
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
		
		ApiCache.putEventGraph(apiClient, serviceId, input, VolumeType.all, null,
				input.pointsWanted, 0, graphActiveTimespan, 0, Response.of(200, activeGraphResult));
		
		if (baselineGraph != null) {
		
			GraphResult baselineGraphResult = new GraphResult();
			baselineGraphResult.graphs = Collections.singletonList(baselineGraph);		
		
			ApiCache.putEventGraph(apiClient, serviceId, baselineInput, VolumeType.all, null,
					input.pointsWanted, regressionInput.baselineTimespan, 0, 0, Response.of(200, baselineGraphResult));
		}
		
		return Pair.of(baselineGraph, activeWindowGraph);
		
	}
		
	protected RegressionOutput createRegressionOutput(EventFilterInput input,
			RegressionInput regressionInput, RegressionWindow regressionWindow,
			RateRegression rateRegression, Map<String, EventResult> eventListMap,
			Graph baseVolumeGraph, Graph activeVolumeGraph, long volume)
	{
		
		RegressionOutput result = new RegressionOutput(false);
		
		result.regressionInput = regressionInput;
		result.regressionWindow = regressionWindow;
		result.rateRegression = rateRegression;
		result.baseVolumeGraph = baseVolumeGraph;
		result.activeVolumeGraph = activeVolumeGraph;
		result.eventListMap = eventListMap;
		result.volume = volume;
		
		if ((regressionInput != null) && (rateRegression != null)) {
			
			result.eventDatas = processRegression(input, regressionInput, rateRegression, true, true);
			
			for (EventData eventData : result.eventDatas)
			{
				
				RegressionData regData = (RegressionData)eventData;
				
				if (regData.event.stats.hits == 0) 
					continue;
				
				switch (regData.type)
				{
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
		
	public RegressionOutput executeRegression(String serviceId, BaseEventVolumeInput input, boolean newOnly)
	{
		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null)
		{
			return RegressionOutput.emptyOutput;
		}
		
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(input.timeFilter);
		
		Pair<RegressionInput, RegressionWindow> regressionInputs =
				getRegressionInput(serviceId, viewId, input, timespan, newOnly);
		
		if (regressionInputs == null)
		{
			return RegressionOutput.emptyOutput;
		}
		
		RegressionInput regressionInput = regressionInputs.getFirst();
		RegressionWindow regressionWindow = regressionInputs.getSecond();
		
		DateTime from = regressionWindow.activeWindowStart;
		DateTime to = regressionWindow.activeWindowStart.plusMinutes(regressionInput.activeTimespan);
		
		Map<String, EventResult> eventListMap = getEventMap(serviceId, input,
			from, to, null, input.pointsWanted);
	
		Graph baselineGraph;
		Graph activeWindowGraph;
		
		if (eventListMap != null)
		{
			Pair<Graph, Graph> regressionGraphs = getRegressionGraphs(serviceId, 
				viewId, regressionInput, regressionWindow, input, newOnly);
			
			baselineGraph = regressionGraphs.getFirst();
			activeWindowGraph = regressionGraphs.getSecond();
		} else {
			baselineGraph = null;
			activeWindowGraph = null;		
		}
		
		if ((activeWindowGraph == null) ||	(activeWindowGraph.points == null)) {
			return RegressionOutput.emptyOutput;
		}
		
		if ((!newOnly) && (((baselineGraph == null) || (baselineGraph.points == null)))) {
			return RegressionOutput.emptyOutput;
		}
		
		Pair<Collection<EventResult>, Long> eventsPair = applyGraphToEvents(serviceId, 
			eventListMap, timespan, activeWindowGraph, input);
		
		Collection<EventResult> events = eventsPair.getFirst();
		long volume = eventsPair.getSecond().longValue();
		
		regressionInput.events = events;
		regressionInput.baselineGraph = baselineGraph;
				
		RegressionOutput result = executeRegression(input, regressionInput, 
			regressionWindow, eventListMap, volume, baselineGraph, activeWindowGraph);
				
		return result;
	}
	
	
	public RegressionOutput executeRegression(BaseEventVolumeInput input, 
			RegressionInput regressionInput, RegressionWindow regressionWindow, 
			Map<String, EventResult> eventListMap, long volume,
			Graph baselineGraph, Graph activeWindowGraph) {
		
		regressionInput.validate();
		
		RateRegression rateRegression = RegressionUtil.calculateRateRegressions(apiClient, regressionInput, null,
				false);
		
		RegressionOutput result = createRegressionOutput(input, 
				regressionInput, regressionWindow,
				rateRegression, eventListMap,
				baselineGraph, activeWindowGraph, volume);
				
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
	
	private EventData mergeRegressionsOfType(List<EventData> eventDatas)
	{
		
		RegressionData first = (RegressionData)(eventDatas.get(0));
		List<EventData> merged = super.mergeEventDatas(eventDatas);
		
		RegressionData result = new RegressionData(first.regResult, first.input, merged.get(0).event, first.type);
		
		long baselineHits = 0;
		long baselineInvocations = 0;
			
		for (EventData eventData : eventDatas)
		{	
			RegressionData regressionData = (RegressionData)eventData;
				
			if (regressionData.regression != null)
			{
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
	protected List<EventData> mergeEventDatas(List<EventData> eventDatas)
	{
		List<EventData> result = new ArrayList<EventData>();
		Map<RegressionType, List<EventData>> regressionMap = getRegressionMap(eventDatas);
		
		for (List<EventData> typeEvents : regressionMap.values()) {
			result.add(mergeRegressionsOfType(typeEvents));
		}
		
		return result;
	}
	
	private List<EventData> doMergeSimilarEvents(String serviceId, List<EventData> eventDatas)
	{
		List<EventData> result = new ArrayList<EventData>(super.mergeSimilarEvents(serviceId, eventDatas));
		sortRegressions(result);
		return result;
	}
	
	@Override
	protected void sortEventDatas(List<EventData> eventDatas)
	{
	}
	
	@Override
	protected List<EventData> mergeSimilarEvents(String serviceId, List<EventData> eventDatas)
	{
		return eventDatas;
	}
	
	public RegressionOutput runRegression(String serviceId, EventFilterInput regInput)
	{
		RegressionOutput regressionOutput = ApiCache.getRegressionOutput(apiClient, 
			serviceId, regInput, this, false, true);
		return regressionOutput;
	}
	
	public RegressionOutput runRegression(String serviceId, EventFilterInput regInput, boolean newOnly)
	{
		RegressionOutput regressionOutput = ApiCache.getRegressionOutput(apiClient, 
			serviceId, regInput, this, newOnly, true);
		return regressionOutput;
	}
	
	@Override
	protected List<EventData> getEventData(String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan)
	{
		
		RegressionsInput regInput = (RegressionsInput)input;
		RegressionOutput regressionOutput = runRegression(serviceId, regInput);
		
		if ((regressionOutput == null) || (regressionOutput.rateRegression == null) ||
			(regressionOutput.regressionInput == null))
		{
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
		
	private double getServiceSingleStat(String serviceId, RegressionsInput input)
	{
		RegressionOutput regressionOutput = runRegression(serviceId, input);
		
		if ((regressionOutput == null) || (regressionOutput.empty))
		{
			return 0;
		}
		
		double result = 0;
		Collection<RegressionType> regressionTypes = input.getRegressionTypes();
		
		for (RegressionType regressionType : regressionTypes) {
			result += regressionOutput.getStat(regressionType);
		}
		
		return result;
	}
	
	private double getSingleStat(Collection<String> serviceIds, RegressionsInput input)
	{
		
		double result = 0;
		
		for (String serviceId : serviceIds)
		{
			result += getServiceSingleStat(serviceId, input);
		}
		
		return result;
	}
	
	private List<Series> processSingleStat(RegressionsInput input)
	{
		
		Collection<String> serviceIds = getServiceIds(input);
		
		if (CollectionUtil.safeIsEmpty(serviceIds))
		{
			return Collections.emptyList();
		}
		
		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		
		Object singleStatText;
		double singleStatValue = getSingleStat(serviceIds, input);
		
		Collection<RegressionType> regressionTypes = input.getRegressionTypes();

		if (regressionTypes == null) {
			return Collections.emptyList();
		}
		
		
		if (input.singleStatFormat != null)
		{
			if (singleStatValue > 0)
			{
				singleStatText = String.format(input.singleStatFormat, String.valueOf((int)singleStatValue));
			}
			else
			{
				singleStatText = EMPTY_POSTFIX;
			}
		}
		else
		{
			singleStatText = Integer.valueOf((int)singleStatValue);
		}
		
		
		return createSingleStatSeries(timeSpan, singleStatText);
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput)
	{
		
		if (!(functionInput instanceof RegressionsInput))
		{
			throw new IllegalArgumentException("functionInput");
		}
		
		RegressionsInput regInput = (RegressionsInput)functionInput;
		
		if (regInput.render == null)
		{
			throw new IllegalStateException("Missing render mode");
		}
		
		switch (regInput.render)
		{
			case Grid:
				return super.process(functionInput);
			
			case Graph:
				throw new IllegalStateException("Graph not supported");
				
			default:
				return processSingleStat(regInput);
			
		}
	}
}
