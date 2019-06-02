package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.RegressionResult;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.RegressedEventsInput;
import com.takipi.integrations.grafana.input.RegressionsInput;

public class RegressedEventsFunction extends EnvironmentVariableFunction {

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
	
	public RegressedEventsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {
		RegressedEventsInput reInput = (RegressedEventsInput)getInput((RegressedEventsInput)input);
		
		Gson gson = new Gson();
		String json = gson.toJson(reInput);
		RegressionsInput rgInput = gson.fromJson(json, RegressionsInput.class);
		
		RegressionFunction regressionFunction = new RegressionFunction(apiClient, settingsMaps);		
		RegressionOutput regressionOutput = regressionFunction.runRegression(serviceId, rgInput, false);
	
		if ((regressionOutput == null) || (regressionOutput.empty)) {
			return;
		}
			
		Collection<RegressionResult> regressionResults = regressionOutput.rateRegression.getSortedAllRegressions();
		
		List<String> values = new ArrayList<String>(regressionResults.size());
		
		for (RegressionResult regressionResult : regressionResults) {
			
			String value = formatLocation(regressionResult.getEvent().error_location);
			
			if ((value != null) && (!values.contains(value))) {
				values.add(value);
			}
		}
		
		for (String value : values) {
			appender.append(getServiceValue(value, serviceId, serviceIds));

		}
	}	
}
