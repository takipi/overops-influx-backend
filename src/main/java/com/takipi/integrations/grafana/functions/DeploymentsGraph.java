package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.client.ClientUtil;
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
				return DeploymentsFunction.compare(o1, o2);
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
	
			List<String> serviceDeployments = getServiceDeps(serviceId);	
			String deployment = getDeployment(dgInput, serviceId, serviceDeployments);
				
			if (deployment == null) {
				continue;
			}
			
			result.add(new GraphAsyncTask(serviceId, viewId, input.view, 
				getInput(dgInput, deployment), timeSpan, serviceIds, pointsWanted));
		
			if (dgInput.graphCount == 0) {
				continue;
			}
			
			int index = serviceDeployments.indexOf(deployment);
			
			if ((index != -1) && (index < serviceDeployments.size() - 1)) {
				
				int seriesCount;
				
				if (index + dgInput.graphCount >= serviceDeployments.size()) {
					seriesCount = serviceDeployments.size() - index - 1;
				} else {
					seriesCount = dgInput.graphCount;
				}
							
				for (int i = 0; i < seriesCount; i++) {
					
					String prevDeployment = serviceDeployments.get(index + 1 + i);
					
					result.add(new GraphAsyncTask(serviceId, viewId, input.view, 
							getInput(dgInput, prevDeployment), timeSpan, serviceIds, pointsWanted));
				}
			}			
		}
		
		return result;
	}
}
