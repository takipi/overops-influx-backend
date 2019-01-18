package com.takipi.integrations.grafana.functions;

import java.util.Collection;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.deployment.SummarizedDeployment;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.DeploymentsInput;
import com.takipi.integrations.grafana.util.DeploymentUtil;

public class DeploymentsFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new DeploymentsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return DeploymentsInput.class;
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
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {

		Collection<SummarizedDeployment> serviceDeps = DeploymentUtil.getDeployments(apiClient, serviceId, false);
		
		if (serviceDeps == null) {
			return;
		}
		
		for (SummarizedDeployment dep : serviceDeps) {

			String depName = getServiceValue(dep.name, serviceId, serviceIds);
			appender.append(depName);
		}
	}

	@Override
	protected int compareValues(String o1, String o2) {
		return DeploymentUtil.compareDeployments(o1, o2);
	}
}
