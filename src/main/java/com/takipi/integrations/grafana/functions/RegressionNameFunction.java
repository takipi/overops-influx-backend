package com.takipi.integrations.grafana.functions;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.integrations.grafana.input.RegressionsNameInput;
import com.takipi.integrations.grafana.input.ViewInput;

public class RegressionNameFunction extends BaseNameFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new RegressionNameFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return RegressionsNameInput.class;
		}

		@Override
		public String getName() {
			return "regressionName";
		}
	}

	public RegressionNameFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected String getName(ViewInput input, String serviceId) {

		RegressionsNameInput regNameInput = (RegressionsNameInput) input;

		RegressionInput regressionInput = new RegressionInput();

		regressionInput.serviceId = serviceId;
		regressionInput.viewId = getViewId(serviceId, input.view);

		if (regressionInput.viewId == null) {
			return null;
		}
		
		regressionInput.activeTimespan = regNameInput.activeTimespan;
		regressionInput.baselineTimespan = regNameInput.minBaselineTimespan;

		regressionInput.applictations = input.getApplications(apiClient, serviceId);
		regressionInput.servers = input.getServers(serviceId);
		regressionInput.deployments = input.getDeployments(serviceId);
		
		/*
		RegressionWindow activeWindow = ApiCache.getRegressionWindow(apiClient, regressionInput);

		int expandedBaselineTimespan = RegressionFunction.expandBaselineTimespan(regNameInput.baselineTimespanFactor,
				regNameInput.minBaselineTimespan, activeWindow);

		regressionInput.activeWindowStart = activeWindow.activeWindowStart;
		regressionInput.activeTimespan = activeWindow.activeTimespan;
		regressionInput.baselineTimespan = expandedBaselineTimespan;

		String result = RegressionStringUtil.getRegressionName(apiClient, regressionInput);
		*/
		
		String result = "";
		return result;
	}
}
