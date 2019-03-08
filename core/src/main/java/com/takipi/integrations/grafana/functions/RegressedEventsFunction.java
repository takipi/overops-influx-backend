package com.takipi.integrations.grafana.functions;

import java.util.Collection;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.RegressionResult;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.RegressedEventsInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.util.ApiCache;

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
		String json = new Gson().toJson(input);
		RegressionsInput rgInput = new Gson().fromJson(json, RegressionsInput.class);
		
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
