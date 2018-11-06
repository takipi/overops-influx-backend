package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.DeploymentsGraphInput;
import com.takipi.integrations.grafana.utils.DeploymentUtil;

public class DeploymentsGraph extends GraphFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new DeploymentsGraph(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return DeploymentsGraphInput.class;
		}

		@Override
		public String getName() {
			return "deploymentsGraph";
		}
	}

	public DeploymentsGraph(ApiClient apiClient) {
		super(apiClient);
	}

	private DeploymentsGraphInput getInput(DeploymentsGraphInput input, String depName) {
		Gson gson = new Gson();
		String json = gson.toJson(input);
		DeploymentsGraphInput result = (DeploymentsGraphInput) gson.fromJson(json, DeploymentsGraphInput.class);
		result.deployments = depName;
		return result;
	}

	@Override
	protected boolean isAsync(String[] serviceIds) {
		return true;
	}

	@Override
	protected Collection<GraphAsyncTask> getTasks(String[] serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {

		DeploymentsGraphInput dgInput = (DeploymentsGraphInput) input;

		List<GraphAsyncTask> result = new ArrayList<GraphAsyncTask>();

		for (String serviceId : serviceIds) {

			String viewId = getViewId(serviceId, input.view);

			if (viewId == null) {
				continue;
			}

			Pair<String, List<String>> deploymentResult = DeploymentUtil.getActiveDeployment(apiClient, dgInput, serviceId,
					dgInput.graphCount);

			result.add(new GraphAsyncTask(serviceId, viewId, input.view, getInput(dgInput, deploymentResult.getFirst()),
					timeSpan, serviceIds, pointsWanted));

			for (String prevDep : deploymentResult.getSecond()) {
				result.add(new GraphAsyncTask(serviceId, viewId, input.view, getInput(dgInput, prevDep), timeSpan,
						serviceIds, pointsWanted));
			}
		}

		return result;
	}
}
