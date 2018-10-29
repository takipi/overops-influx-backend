package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
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
import com.takipi.integrations.grafana.utils.TimeUtils;

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
		
		protected RegressionData(EventResult event, String type) {
			super(event);
			this.type = type;
		}
		
		protected RegressionData(RegressionResult regression, String type) {
			this(regression.getEvent(), type);
			this.regression = regression;
		}	
	}
	
	protected static class RegressionFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			RegressionData regData = (RegressionData)eventData;
			
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

			RegressionData regData = (RegressionData)eventData;		
			return regData.type;
		}
	}

	public RegressionFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private List<EventData> processRegressionData(String serviceId, RateRegression rateRegression,
			RegressionsInput input, Pair<DateTime, DateTime> timespan) {

		List<EventData> result = new ArrayList<EventData>();

		for (EventResult event : rateRegression.getExceededNewEvents().values()) {
			result.add(new RegressionData(event, RegressionStringUtil.SEVERE_NEW));
		}

		for (EventResult event : rateRegression.getCriticalNewEvents().values()) {
			result.add(new RegressionData(event, RegressionStringUtil.SEVERE_NEW));
		}

		for (EventResult event : rateRegression.getAllNewEvents().values()) {

			if (rateRegression.getExceededNewEvents().containsKey(event.id)) {
				continue;
			}

			if (rateRegression.getCriticalNewEvents().containsKey(event.id)) {
				continue;
			}

			result.add(new RegressionData(event, RegressionStringUtil.NEW_ISSUE));

		}

		for (RegressionResult regressionResult : rateRegression.getCriticalRegressions().values()) {
			result.add(new RegressionData(regressionResult, RegressionStringUtil.SEVERE_REGRESSION));
		}

		for (RegressionResult regressionResult : rateRegression.getAllRegressions().values()) {

			if (rateRegression.getCriticalRegressions().containsKey(regressionResult.getEvent().id)) {
				continue;
			}

			result.add(new RegressionData(regressionResult, RegressionStringUtil.REGRESSION));
		}

		return result;
	}

	private DateTime getDeploymentStartTime(RegressionsInput input, String serviceId, String viewId,
			Pair<DateTime, DateTime> timespan) {

		Graph graph = getEventsGraph(apiClient, serviceId, viewId, 10, input, VolumeType.hits, timespan.getFirst(),
				timespan.getSecond());

		if (graph == null) {
			return null;
		}

		DateTime result = TimeUtils.getDateTime(graph.points.get(0).time);

		return result;
	}
	
	@Override
	protected FieldFormatter getFormatter(String column) {
		
		if (column.equals(REGRESSION)) {
			return new RegressionFormatter();
		}

		if (column.equals(ISSUE_TYPE)) {
			return new RegressionTypeFormatter();
		}
		
		return super.getFormatter(column);
	}

	@Override
	protected Collection<EventData> getEventData(String serviceId, EventsInput input, 
		Pair<DateTime, DateTime> timeSpan) {
		
		String viewId = getViewId(serviceId, input.view);

		if (viewId == null) {
			return Collections.emptyList();
		}

		RegressionsInput regInput = (RegressionsInput)input;
		
		RegressionInput regressionInput = new RegressionInput();

		regressionInput.serviceId = serviceId;
		regressionInput.viewId = viewId;
		regressionInput.baselineTimespan = regInput.baselineTimespan;
		regressionInput.minVolumeThreshold = regInput.minVolumeThreshold;
		regressionInput.minErrorRateThreshold = regInput.minErrorRateThreshold;
		regressionInput.regressionDelta = regInput.regressionDelta;
		regressionInput.criticalRegressionDelta = regInput.criticalRegressionDelta;
		regressionInput.applySeasonality = regInput.applySeasonality;
		regressionInput.criticalExceptionTypes = regInput.getCriticalExceptionTypes();

		regressionInput.applictations = input.getApplications(serviceId);
		regressionInput.servers = input.getServers(serviceId);
		regressionInput.deployments = input.getDeployments(serviceId);

		int activeTimespan;

		if (regressionInput.deployments.size() > 0) {

			DateTime depStartTime = getDeploymentStartTime(regInput, serviceId, viewId, timeSpan);

			if (depStartTime != null) {
				DateTime timeDelta = DateTime.now().minus(depStartTime.getMillis());
				activeTimespan = TimeUtils.toMinutes(timeDelta.getMillis());
			} else {
				activeTimespan = regInput.activeTimespan;
			}
		} else {
			activeTimespan = regInput.activeTimespan;
		}

		regressionInput.activeTimespan = activeTimespan;

		regressionInput.validate();

		RateRegression rateRegression = RegressionUtil.calculateRateRegressions(apiClient, regressionInput, System.out,
				false);

		return processRegressionData(serviceId, rateRegression, regInput, timeSpan);
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof RegressionsInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		return super.process(functionInput);
	}
}
