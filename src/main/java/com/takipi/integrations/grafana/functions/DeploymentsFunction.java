package com.takipi.integrations.grafana.functions;

import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.util.DeploymentUtil;

public class DeploymentsFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new DeploymentsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EnvironmentsInput.class;
		}

		@Override
		public String getName() {
			return "deployments";
		}
	}

	public DeploymentsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, String[] serviceIds, String serviceId,
			VariableAppender appender) {

		List<String> serviceDeps = ClientUtil.getDeployments(apiClient, serviceId);

		for (String dep : serviceDeps) {

			String depName = getServiceValue(dep, serviceId, serviceIds);
			appender.append(depName);
		}
	}

	@Override
	protected int compareValues(Object o1, Object o2) {
		return DeploymentUtil.compareDeployments(o1, o2);
	}
}
