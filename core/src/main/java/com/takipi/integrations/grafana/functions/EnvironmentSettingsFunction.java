
package com.takipi.integrations.grafana.functions;

import java.util.Collection;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.EnvironmentsInput;

public class EnvironmentSettingsFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new EnvironmentSettingsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EnvironmentsInput.class;
		}

		@Override
		public String getName() {
			return "environmentSettings";
		}
	}

	public EnvironmentSettingsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {
		
		throw new IllegalStateException("Support removed");
	}

}
