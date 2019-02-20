package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.ServersInput;

public class ServersFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ServersFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return ServersInput.class;
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
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {

		List<String> serviceServers;
		
		try {
			serviceServers = ClientUtil.getServers(apiClient, serviceId);
		} catch (Exception e) {
			System.err.println(e);
			return;
		}

		for (String server : serviceServers) {

			String serverName = getServiceValue(server, serviceId, serviceIds);
			appender.append(serverName);
		}
	}
}
