package com.takipi.integrations.grafana.functions;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.ConcatInput;
import com.takipi.integrations.grafana.input.FunctionInput;

public class ConcatFunction extends VariableFunction {

	public ConcatFunction(ApiClient apiClient) {
		super(apiClient);
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ConcatFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return ConcatInput.class;
		}

		@Override
		public String getName() {
			return "concat";
		}
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {
		ConcatInput ciInput = (ConcatInput)input;
		appender.append(ciInput.a + ciInput.separator + ciInput.b);
	}
}


