package com.takipi.integrations.grafana.functions;

import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.service.SummarizedService;
import com.takipi.common.udf.util.ApiFilterUtil;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.VariableInput;

public class EnvironmentsFunction extends VariableFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new EnvironmentsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return VariableInput.class;
		}

		@Override
		public String getName() {
			return "environments";
		}
	}

	public EnvironmentsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {

		List<SummarizedService> services = ApiFilterUtil.getEnvironments(apiClient);

		for (SummarizedService service : services) {
			appender.append(getServiceValue(service.name, service.id));
		}
	}
}
