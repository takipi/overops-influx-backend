package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.regression.RateRegression;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionResult;
import com.takipi.api.client.util.regression.RegressionStringUtil;
import com.takipi.api.client.util.regression.RegressionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.output.Series;
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

		protected RegressionData(RateRegression regResult, EventResult event, String type) {
			super(event);
			this.type = type;
			this.regResult = regResult;
		}

		protected RegressionData(RateRegression regResult, RegressionResult regression, String type) {
			this(regResult, regression.getEvent(), type);
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

	protected static class RegressionLinkFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			RegressionData regData = (RegressionData) eventData;
			RegressionsInput regInput = (RegressionsInput)input;

			DateTime from = regData.regResult.getActiveWndowStart().minusMinutes(regInput.baselineTimespan);
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

	private List<EventData> processRegressionData(RateRegression rateRegression) {
		List<EventData> result = new ArrayList<EventData>();

		for (EventResult event : rateRegression.getExceededNewEvents().values()) {
			result.add(new RegressionData(rateRegression, event, RegressionStringUtil.SEVERE_NEW));
		}

		for (EventResult event : rateRegression.getCriticalNewEvents().values()) {
			result.add(new RegressionData(rateRegression, event, RegressionStringUtil.SEVERE_NEW));
		}

		for (EventResult event : rateRegression.getAllNewEvents().values()) {
			if (rateRegression.getExceededNewEvents().containsKey(event.id)) {
				continue;
			}

			if (rateRegression.getCriticalNewEvents().containsKey(event.id)) {
				continue;
			}

			result.add(new RegressionData(rateRegression, event, RegressionStringUtil.NEW_ISSUE));

		}

		for (RegressionResult regressionResult : rateRegression.getCriticalRegressions().values()) {
			result.add(new RegressionData(rateRegression, regressionResult, RegressionStringUtil.SEVERE_REGRESSION));
		}

		for (RegressionResult regressionResult : rateRegression.getAllRegressions().values()) {

			if (rateRegression.getCriticalRegressions().containsKey(regressionResult.getEvent().id)) {
				continue;
			}

			result.add(new RegressionData(rateRegression, regressionResult, RegressionStringUtil.REGRESSION));
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

	@Override
	protected Collection<EventData> getEventData(String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan) {

		String viewId = getViewId(serviceId, input.view);

		if (viewId == null) {
			return Collections.emptyList();
		}

		RegressionsInput regInput = (RegressionsInput) input;

		RegressionInput regressionInput = new RegressionInput();

		regressionInput.serviceId = serviceId;
		regressionInput.viewId = viewId;

		regressionInput.activeTimespan = regInput.activeTimespan;
		regressionInput.baselineTimespan = regInput.baselineTimespan;

		regressionInput.criticalExceptionTypes = regInput.getCriticalExceptionTypes();
		regressionInput.minVolumeThreshold = regInput.minVolumeThreshold;
		regressionInput.minErrorRateThreshold = regInput.minErrorRateThreshold;

		regressionInput.regressionDelta = regInput.regressionDelta;
		regressionInput.criticalRegressionDelta = regInput.criticalRegressionDelta;
		regressionInput.applySeasonality = regInput.applySeasonality;

		regressionInput.applictations = input.getApplications(serviceId);
		regressionInput.servers = input.getServers(serviceId);
		regressionInput.deployments = input.getDeployments(serviceId);

		regressionInput.validate();

		RateRegression rateRegression = RegressionUtil.calculateRateRegressions(apiClient, regressionInput, System.out,
				false);

		return processRegressionData(rateRegression);
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof RegressionsInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		return super.process(functionInput);
	}
}
