
package com.takipi.integrations.grafana.functions;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.utils.DeploymentUtil;

public class DeploymentNameFunction extends BaseNameFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new DeploymentNameFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return ViewInput.class;
		}

		@Override
		public String getName() {
			return "deploymentName";
		}
	}

	public DeploymentNameFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected String getName(ViewInput input, String serviceId) {
		return DeploymentUtil.getActiveDeployment(apiClient, input, serviceId);
	}
}
