package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.deployment.SummarizedDeployment;
import com.takipi.api.client.request.deployment.DeploymentsRequest;
import com.takipi.api.client.result.deployment.DeploymentsResult;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.DeploymentsGraphInput;

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
		DeploymentsGraphInput result = (DeploymentsGraphInput)gson.fromJson(json, DeploymentsGraphInput.class);
		result.deployments = depName;
		return result;
	}
	
	private String getDeployment(DeploymentsGraphInput input, String serviceId, 
		List<String> serviceDeployments) {
		
		List<String> inputDeployments = input.getDeployments(serviceId);
		
		String result;
		
		if (inputDeployments.size() == 1) {
			result = inputDeployments.get(0);
		} else {
			
			if (serviceDeployments.size() == 0) {
				return null;
			}
			
			result = serviceDeployments.get(0);
		}
		
		return result;
	}
	
	private List<String> getServiceDeps(String serviceId) {
		
		List<String> result = ClientUtil.getDeployments(apiClient, serviceId);
		
		result.sort(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return compareDeployments(o1, o2);
			}
		});
		
		return result;
	}
	
	protected String getSeriesName(BaseGraphInput input, String seriesName, Object volumeType, String serviceId, String[] serviceIds) {
	
		return getServiceValue(input.deployments, serviceId, serviceIds);
	}
	
	@Override
	protected boolean isAsync(String[] serviceIds) {
		return true;
	}
	
	private List<String> getActiveDeployments(String serviceId) {
		
		DeploymentsRequest request = DeploymentsRequest.newBuilder().setServiceId(serviceId).setActive(true).build();

		Response<DeploymentsResult> response = apiClient.get(request);

		if ((response.isBadResponse()) || (response.data == null)) {
			throw new IllegalStateException(
					"Could not acquire deployments for service " + serviceId + " . Error " + response.responseCode);
		}

		if (response.data.deployments == null) {
			return Collections.emptyList();
		}

		List<String> result = Lists.newArrayListWithCapacity(response.data.deployments.size());

		for (SummarizedDeployment deployment : response.data.deployments) {
			result.add(deployment.name);
		}

		return result;
	}
	
	@Override
	protected Collection<GraphAsyncTask> getTasks(String[] serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {
		
		DeploymentsGraphInput dgInput = (DeploymentsGraphInput)input;

		List<GraphAsyncTask> result  = new ArrayList<GraphAsyncTask>();
		
		for (String serviceId : serviceIds) {
		
			String viewId = getViewId(serviceId, input.view);
			
			if (viewId == null) {
				continue;
			}

			List<String> activeDeployments = getActiveDeployments(serviceId);
			
			String deployment = getDeployment(dgInput, serviceId, activeDeployments);
				
			List<String> allDeployments = getServiceDeps(serviceId);	
			
			if (allDeployments == null) {
				continue;
			}
			
			if (deployment == null) {
				continue;
			}
			
			result.add(new GraphAsyncTask(serviceId, viewId, input.view, 
				getInput(dgInput, deployment), timeSpan, serviceIds, pointsWanted));
		
			if (dgInput.graphCount == 0) {
				continue;
			}
			
			int index = allDeployments.indexOf(deployment);
			
			if ((index != -1) && (index < allDeployments.size() - 1)) {
				
				int seriesCount;
				
				if (index + dgInput.graphCount >= allDeployments.size()) {
					seriesCount = allDeployments.size() - index - 1;
				} else {
					seriesCount = dgInput.graphCount;
				}
							
				for (int i = 0; i < seriesCount; i++) {
					
					String prevDeployment = allDeployments.get(index + 1 + i);
					
					result.add(new GraphAsyncTask(serviceId, viewId, input.view, 
							getInput(dgInput, prevDeployment), timeSpan, serviceIds, pointsWanted));
				}
			}			
		}
		
		return result;
	}
}
