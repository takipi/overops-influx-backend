package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.regression.RateRegression;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionResult;
import com.takipi.api.client.util.regression.RegressionStringUtil;
import com.takipi.api.client.util.regression.RegressionUtil;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.EventSettings;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.RegressionSettings;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.EventLinkEncoder;

public class RegressionFunction extends EventsFunction {

	private static String REGRESSION = "regression";
	private static String ISSUE_TYPE = "issueType";

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
		
		protected String type;
		protected RegressionResult regression;
		protected RateRegression regResult;
		protected RegressionInput input;

		protected RegressionData(RateRegression regResult, RegressionInput input, EventResult event, String type) {
			super(event);
			this.type = type;
			this.regResult = regResult;
			this.input = input;
			this.regression = null;
		}

		protected RegressionData(RateRegression regResult, RegressionInput input, RegressionResult regression,
				String type) {
			this(regResult, input, regression.getEvent(), type);
			this.regression = regression;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (!super.equals(obj)) {
				return false;
			}
			
			RegressionData other = (RegressionData)obj;
			
			if (!Objects.equal(type, other.type)) {
				return false;
			}
			
			return true;
 		}
	}

	protected static class RegressionRateFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			RegressionData regData = (RegressionData) eventData;

			if (regData.regression != null) {
				return RegressionStringUtil.getRegressedEventRate(regData.regression);
			} else {
				return RegressionStringUtil.getEventRate(regData.event);
			}
		}
	}

	protected static class RegressionTypeFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			RegressionData regData = (RegressionData) eventData;
			return regData.type;
		}
	}

	public static int expandBaselineTimespan(int baselineTimespanFactor, int minBaselineTimespan,
			RegressionWindow activeWindow) {

		int result;
		double factor = (double) minBaselineTimespan / (double) activeWindow.activeTimespan;

		if (factor > baselineTimespanFactor) {
			result = minBaselineTimespan;
		} else {
			result = activeWindow.activeTimespan * baselineTimespanFactor;
		}

		if (result < 0) {
			System.out.println();
		}
		return result;

	}

	protected static class RegressionLinkFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			RegressionData regData = (RegressionData) eventData;

			DateTime from = regData.regResult.getActiveWndowStart().minusMinutes(regData.input.baselineTimespan);
			DateTime to = DateTime.now();

			return EventLinkEncoder.encodeLink(serviceId, input, eventData.event, from, to);
		}

		@Override
		protected Object formatValue(Object value, EventsInput input) {
			return value;
		}
	}

	public RegressionFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	private void sortRegressions(String serviceId, List<EventData> eventData) {
		
		EventSettings displaySettings = GrafanaSettings.getServiceSettings(apiClient, serviceId).gridSettings; 
		
		if ((displaySettings == null) || (displaySettings.regressionSortOrder == null)) {
			return;
		}
		
		List<String> order = Arrays.asList(displaySettings.regressionSortOrder.split(GrafanaFunction.ARRAY_SEPERATOR));
		List<String> types = Arrays.asList(displaySettings.regressionTypeOrder.split(GrafanaFunction.ARRAY_SEPERATOR));

		eventData.sort(new Comparator<EventData>() {

			@Override
			public int compare(EventData o1, EventData o2) {
				
				RegressionData r1 = (RegressionData)o1;
				RegressionData r2 = (RegressionData)o2;
				
				int result = order.indexOf(r1.type) - order.indexOf(r2.type);

				if (result != 0) {
					return result;
				}
				
				result = types.indexOf(r1.event.type) - types.indexOf(r2.event.type);

				if (result != 0) {
					return result;
				}
				
				return (int)(o2.event.stats.hits - o1.event.stats.hits);
			}
		});
	}

	private List<EventData> processRegressionData(RegressionInput input,
			RateRegression rateRegression) {

		List<EventData> result = new ArrayList<EventData>();

		for (EventResult event : rateRegression.getSortedCriticalNewEvents()) {
			result.add(new RegressionData(rateRegression, input, event, RegressionStringUtil.SEVERE_NEW));
		}

		for (EventResult event : rateRegression.getSortedExceededNewEvents()) {
			result.add(new RegressionData(rateRegression, input, event, RegressionStringUtil.SEVERE_NEW));
		}

		for (EventResult event : rateRegression.getSortedAllNewEvents()) {

			if (rateRegression.getExceededNewEvents().containsKey(event.id)) {
				continue;
			}

			if (rateRegression.getCriticalNewEvents().containsKey(event.id)) {
				continue;
			}

			result.add(new RegressionData(rateRegression, input, event, RegressionStringUtil.NEW_ISSUE));

		}

		for (RegressionResult regressionResult : rateRegression.getSortedCriticalRegressions()) {
			result.add(new RegressionData(rateRegression, input, regressionResult,
					RegressionStringUtil.SEVERE_REGRESSION));
		}

		for (RegressionResult regressionResult : rateRegression.getSortedAllRegressions()) {

			if (rateRegression.getCriticalRegressions().containsKey(regressionResult.getEvent().id)) {
				continue;
			}

			result.add(new RegressionData(rateRegression, input, regressionResult, RegressionStringUtil.REGRESSION));
		}
		
		return result;
	}

	@Override
	protected FieldFormatter getFormatter(String column) {

		if (column.equals(REGRESSION)) {
			return new RegressionRateFormatter();
		}

		if (column.equals(ISSUE_TYPE)) {
			return new RegressionTypeFormatter();
		}

		if (column.equals(LINK)) {
			return new RegressionLinkFormatter();
		}

		return super.getFormatter(column);
	}

	protected static class RegressionOutput {

		protected RegressionInput regressionInput;
		protected RateRegression rateRegression;
		protected Graph baseVolumeGraph;
		protected Graph activeVolumeGraph;

		long volume;

	}

	protected RegressionOutput executeRegression(String serviceId, RegressionsInput input,
			Pair<DateTime, DateTime> timeSpan) {

		String viewId = getViewId(serviceId, input.view);

		if (viewId == null) {
			return null;
		}

		RegressionSettings regressionSettings = GrafanaSettings.getServiceSettings(apiClient, serviceId).regressionSettings;
		
		if (regressionSettings == null) {
			throw new IllegalStateException("Missing ergression settings for " + serviceId);
		}
			
		RegressionInput regressionInput = new RegressionInput();

		regressionInput.serviceId = serviceId;
		regressionInput.viewId = viewId;
		regressionInput.deployments = input.getDeployments(serviceId);

		regressionInput.activeTimespan = (int) TimeUnit.MILLISECONDS
				.toMinutes(timeSpan.getSecond().getMillis() - timeSpan.getFirst().getMillis());

		regressionInput.baselineTimespan = regressionSettings.minBaselineTimespan;

		RegressionWindow regressionWindow = ApiCache.getRegressionWindow(apiClient, regressionInput);

		if ((!CollectionUtil.safeIsEmpty(regressionInput.deployments)) 
			&& (!regressionWindow.deploymentFound)) {
			return null;
		}

		int expandedBaselineTimespan = expandBaselineTimespan(regressionSettings.baselineTimespanFactor, regressionSettings.minBaselineTimespan,
				regressionWindow);

		regressionInput.activeWindowStart = regressionWindow.activeWindowStart;
		regressionInput.activeTimespan = regressionWindow.activeTimespan;
		regressionInput.baselineTimespan = expandedBaselineTimespan;

		regressionInput.applictations = input.getApplications(apiClient, serviceId);
		regressionInput.servers = input.getServers(serviceId);

		regressionInput.criticalExceptionTypes = input.getCriticalExceptionTypes(apiClient, serviceId);
		regressionInput.minVolumeThreshold = regressionSettings.minVolumeThreshold;
		regressionInput.minErrorRateThreshold = regressionSettings.minErrorRateThreshold;

		regressionInput.regressionDelta = regressionSettings.regressionDelta;
		regressionInput.criticalRegressionDelta = regressionSettings.criticalRegressionDelta;
		regressionInput.applySeasonality = regressionSettings.applySeasonality;

		Map<String, EventResult> eventListMap = getEventMap(serviceId, input, timeSpan.getFirst(), timeSpan.getSecond(),
				null, input.pointsWanted);

		int ratioBaselinePoints = (regressionInput.baselineTimespan / regressionWindow.activeTimespan) * 2;
		int baselineDays = (int) TimeUnit.MINUTES.toDays(regressionInput.baselineTimespan);

		int baselinePoints = Math.min(ratioBaselinePoints, baselineDays / 3);

		if (baselinePoints <= 0) {
			throw new IllegalStateException("Negative points for dep " + Arrays.toString(regressionInput.deployments.toArray()) + " "
					+ ratioBaselinePoints + " " + baselineDays + "  " + regressionWindow.activeWindowStart);
		}
		
		Graph baselineGraph = getEventsGraph(apiClient, serviceId, viewId, baselinePoints, input, VolumeType.all,
				regressionWindow.activeWindowStart.minusMinutes(expandedBaselineTimespan),
				regressionWindow.activeWindowStart);

		Graph activeWindowGraph = getEventsGraph(apiClient, serviceId, viewId, input.pointsWanted, input,
				VolumeType.all, regressionWindow.activeWindowStart, DateTime.now());

		if ((eventListMap == null) || (baselineGraph == null) || (activeWindowGraph == null)
				|| (baselineGraph.points == null) || (activeWindowGraph.points == null)) {
			return null;
		}

		long volume = 0;

		for (GraphPoint gp : activeWindowGraph.points) {

			if (gp.contributors == null) {
				continue;
			}

			for (GraphPointContributor gpc : gp.contributors) {

				EventResult event = eventListMap.get(gpc.id);

				if (event != null) {
					event.stats.invocations += gpc.stats.invocations;
					event.stats.hits += gpc.stats.hits;
					volume += gpc.stats.hits;
				}
			}
		}

		EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);

		List<EventResult> events = new ArrayList<EventResult>();

		for (EventResult event : eventListMap.values()) {

			if (eventFilter.filter(event)) {
				continue;
			}

			events.add(event);
		}

		regressionInput.events = events;
		regressionInput.baselineGraph = baselineGraph;

		regressionInput.validate();

		RateRegression rateRegression = RegressionUtil.calculateRateRegressions(apiClient, regressionInput, System.out,
				false);

		RegressionOutput result = new RegressionOutput();

		result.rateRegression = rateRegression;
		result.regressionInput = regressionInput;
		result.volume = volume;
		result.activeVolumeGraph = activeWindowGraph;
		result.baseVolumeGraph = baselineGraph;

		return result;
	}
	
	@Override
	protected EventData mergeEventDatas(List<EventData> eventDatas) {
		
		RegressionData first = (RegressionData)(eventDatas.get(0));
		EventData merged = super.mergeEventDatas(eventDatas);
		
		RegressionData result = new RegressionData(first.regResult, first.input, merged.event, first.type);
		
		if (first.regression != null) {
		
			long baselineHits = 0;
			long baselineInvocations = 0;
			
			for (EventData eventData : eventDatas) {
				
				RegressionData regressionData = (RegressionData)eventData;
				
				if (regressionData.regression != null) {
					baselineHits += regressionData.regression.getBaselineHits();
					baselineInvocations += regressionData.regression.getBaselineInvocations();
				}
			}
			
			result.regression = RegressionResult.of(result.event, baselineHits, baselineInvocations);
		}
		
		return result;
	}
	
	@Override
	protected Collection<EventData> mergeSimilarEvents(String serviceId, Collection<EventData> eventDatas) {
		List<EventData> result = new ArrayList<EventData>(super.mergeSimilarEvents(serviceId, eventDatas));
		sortRegressions(serviceId, result);
		return result;
	}

	@Override
	protected Collection<EventData> getEventData(String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan) {

		RegressionsInput regInput = (RegressionsInput) input;
		RegressionOutput regressionOutput = executeRegression(serviceId, regInput, timeSpan);

		if (regressionOutput == null) {
			return Collections.emptySet();
		}

		return processRegressionData(regressionOutput.regressionInput, regressionOutput.rateRegression);
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof RegressionsInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		return super.process(functionInput);
	}
}
