package com.takipi.integrations.grafana.functions;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TimeFilterInput;

public class TimeFilterFunction extends VariableFunction {
	
	public TimeFilterFunction(ApiClient apiClient) {
		super(apiClient);
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TimeFilterFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TimeFilterInput.class;
		}

		@Override
		public String getName() {
			return "timeFilter";
		}
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {
		
		TimeFilterInput tfInput = (TimeFilterInput)input;
		appender.append(tfInput.timeFilter);
	}
}