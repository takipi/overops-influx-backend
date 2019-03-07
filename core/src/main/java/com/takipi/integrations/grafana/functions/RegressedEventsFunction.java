package com.takipi.integrations.grafana.functions;

import java.util.Collection;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.RegressionResult;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.RegressedEventsInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.TimeUtil;

public class RegressedEventsFunction extends EnvironmentVariableFunction
{

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new RegressedEventsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return RegressedEventsInput.class;
		}

		@Override
		public String getName() {
			return "increasingErrors";
		}
	}
	public RegressedEventsFunction(ApiClient apiClient)
	{
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender)
	{
		RegressedEventsInput reInput = (RegressedEventsInput)input;
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(reInput.timeFilter);
		
		Gson gson = new Gson();
		String json = gson.toJson(input);
		RegressionsInput rgInput = gson.fromJson(json, RegressionsInput.class);
		rgInput.timeFilter = TimeUtil.getTimeFilter(timespan);
		
		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		
		RegressionOutput regressionOutput = ApiCache.getRegressionOutput(apiClient, 
			serviceId, rgInput, regressionFunction, false, true);
				
		if (regressionOutput == null) {
			return;
		}
		
		for (RegressionResult regressionResult : regressionOutput.rateRegression.getSortedAllRegressions()) {
			String value = formatLocation(regressionResult.getEvent().error_location);
			
			if (value != null) {
				appender.append(getServiceValue(value, serviceId, serviceIds));
			}
		}
	}
	
}
