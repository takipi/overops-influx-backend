package com.takipi.integrations.grafana.functions;

import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.common.udf.util.ApiFilterUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;

public class ApplicationsFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ApplicationsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EnvironmentsInput.class;
		}

		@Override
		public String getName() {
			return "applications";
		}
	}

	public ApplicationsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, String[] serviceIds, String serviceId,
			VariableAppender appender) {

		List<String> serviceApps = ApiFilterUtil.getApplications(apiClient, serviceId);

		for (String app : serviceApps) {

			String serviceApp = getServiceValue(app, serviceId, serviceIds);
			appender.append(serviceApp);
		}
	}
}
