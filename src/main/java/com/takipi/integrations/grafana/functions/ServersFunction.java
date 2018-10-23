package com.takipi.integrations.grafana.functions;

import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.common.udf.util.ApiFilterUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;

public class ServersFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ServersFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EnvironmentsInput.class;
		}

		@Override
		public String getName() {
			return "servers";
		}
	}

	public ServersFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, String[] serviceIds, String serviceId,
			VariableAppender appender) {

		List<String> serviceServers = ApiFilterUtil.getSevers(apiClient, serviceId);

		for (String server : serviceServers) {

			String serverName = getServiceValue(server, serviceId, serviceIds);
			appender.append(serverName);
		}
	}
}
