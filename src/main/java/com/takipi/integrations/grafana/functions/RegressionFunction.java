package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

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
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.EventLinkEncoder;

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

		protected RegressionData(RateRegression regResult, RegressionInput input,
			RegressionResult regression, String type) {
			this(regResult, input, regression.getEvent(), type);
			this.regression = regression;
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
			Pair<DateTime, Integer> activeWindow) {
		
		int result;
		double factor = (double)minBaselineTimespan / (double)activeWindow.getSecond().intValue();
		
		if (factor > baselineTimespanFactor) {
			result = minBaselineTimespan;
		} else {
			result = activeWindow.getSecond().intValue() * baselineTimespanFactor;
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

	private List<EventData> processRegressionData(String serviceId, RegressionInput input,
		RateRegression rateRegression, Pair<DateTime, DateTime> timespan) {

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
			result.add(new RegressionData(rateRegression, input, regressionResult, RegressionStringUtil.SEVERE_REGRESSION));
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
		long volume;
		
	}
	
	protected RegressionOutput executeRegerssion(String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan) {
		
		String viewId = getViewId(serviceId, input.view);

		if (viewId == null) {
			return null;
		}

		RegressionsInput regInput = (RegressionsInput) input;
		
		RegressionInput regressionInput = new RegressionInput();

		regressionInput.serviceId = serviceId;
		regressionInput.viewId = viewId;
		regressionInput.deployments = input.getDeployments(serviceId);

		regressionInput.activeTimespan = (int)TimeUnit.MILLISECONDS.toMinutes(timeSpan.getSecond().getMillis() -
			timeSpan.getFirst().getMillis());
		
		regressionInput.baselineTimespan = regInput.minBaselineTimespan;

		Pair<DateTime, Integer> activeWindow = RegressionUtil.getActiveWindow(apiClient, regressionInput, System.out);
		
		int expandedBaselineTimespan = expandBaselineTimespan(regInput.baselineTimespanFactor, 
			regInput.minBaselineTimespan, activeWindow);

		regressionInput.activeWindowStart = activeWindow.getFirst();
		regressionInput.activeTimespan = activeWindow.getSecond().intValue();
		regressionInput.baselineTimespan = expandedBaselineTimespan;

		regressionInput.applictations = input.getApplications(serviceId);
		regressionInput.servers = input.getServers(serviceId);

		regressionInput.criticalExceptionTypes = regInput.getCriticalExceptionTypes();
		regressionInput.minVolumeThreshold = regInput.minVolumeThreshold;
		regressionInput.minErrorRateThreshold = regInput.minErrorRateThreshold;

		regressionInput.regressionDelta = regInput.regressionDelta;
		regressionInput.criticalRegressionDelta = regInput.criticalRegressionDelta;
		regressionInput.applySeasonality = regInput.applySeasonality;

		Map<String, EventResult> eventListMap = getEventMap(serviceId, regInput, timeSpan.getFirst(),
				timeSpan.getSecond(), null, input.pointsWanted);

		int baselinePoints = regressionInput.baselineTimespan / activeWindow.getSecond().intValue() * 2;
		
		Graph baselineGraph = getEventsGraph(apiClient, serviceId, viewId, baselinePoints, regInput, 
				VolumeType.all, activeWindow.getFirst().minusMinutes(expandedBaselineTimespan), activeWindow.getFirst());

		Graph activeWindowGraph = getEventsGraph(apiClient, serviceId, viewId, input.pointsWanted, regInput,
				VolumeType.all, activeWindow.getFirst(), DateTime.now());

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

		EventFilter eventFilter = input.getEventFilter(serviceId);

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
		
		return result;
	}
	
	@Override
	protected Collection<EventData> getEventData(String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan) {

		RegressionOutput regressionOutput = executeRegerssion(serviceId, input, timeSpan);

		if (regressionOutput == null) {
			return Collections.emptySet();
		}
		
		return processRegressionData(serviceId, regressionOutput.regressionInput, 
				regressionOutput.rateRegression, timeSpan);
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof RegressionsInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		return super.process(functionInput);
	}
}
