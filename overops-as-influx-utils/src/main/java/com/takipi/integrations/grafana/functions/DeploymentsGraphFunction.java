package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.deployment.SummarizedDeployment;
import com.takipi.api.client.result.deployment.DeploymentsResult;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.DeploymentsGraphInput;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.TimeUtil;

public class DeploymentsGraphFunction extends GraphFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new DeploymentsGraphFunction(apiClient);
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

	public DeploymentsGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private DeploymentsGraphInput getInput(DeploymentsGraphInput input, String depName) {
		Gson gson = new Gson();
		String json = gson.toJson(input);
		DeploymentsGraphInput result = gson.fromJson(json, DeploymentsGraphInput.class);
		result.deployments = depName;
		return result;
	}
	
	@Override
	protected String getSeriesName(BaseGraphInput input, String seriesName, String serviceId, Collection<String> serviceIds) {
		return getServiceValue(input.deployments, serviceId, serviceIds);	
	}

	@Override
	protected boolean isAsync(Collection<String> serviceIds) {
		return true;
	}
	
	private Collection<String> getDeployementNames(Collection<SummarizedDeployment> deployments) {
		
		List<String> result = new ArrayList<String>(deployments.size());
		
		for (SummarizedDeployment sd : deployments) {
			result.add(sd.name);
		}
		
		return result;
	}

	private Collection<String> getDeployments(String serviceId, DeploymentsGraphInput input) {
		
		Collection<String> selectedDeployments = input.getDeployments(serviceId);
		
		if (!CollectionUtil.safeIsEmpty(selectedDeployments)) {
			return selectedDeployments;
		}
		
		Response<DeploymentsResult> response = ApiCache.getDeployments(apiClient, serviceId, false);
		
		if ((response == null) || (response.data == null) || (response.data.deployments == null)) {
			return Collections.emptyList();
		}
		
		if ((input.limit == 0) || (input.limit > response.data.deployments.size())) {
			return getDeployementNames(response.data.deployments);
		}
		
		List<SummarizedDeployment> sorted = new ArrayList<>(response.data.deployments);
		
		sorted.sort(new Comparator<SummarizedDeployment>()
		{

			@Override
			public int compare(SummarizedDeployment o1, SummarizedDeployment o2)
			{
				long t1;
				long t2;
				
				if (o1.last_seen != null) {
					t1 = TimeUtil.getLongTime(o1.last_seen);
				} else {
					t1 = 0;
				}
				
				if (o2.last_seen != null) {
					t2 = TimeUtil.getLongTime(o2.last_seen);
				} else {
					t2 = 0;
				}
				
				if (t2 > t1) {
					return 1;
				}
				
				if (t2 < t1) {
					return-1;
				}
				
				return 0;
			}
		});
		
		return getDeployementNames(sorted.subList(0, 
			Math.min(input.limit,sorted.size())));
	}
	
	
	@Override
	protected Collection<Callable<Object>> getTasks(Collection<String> serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {

		DeploymentsGraphInput dgInput = (DeploymentsGraphInput) input;

		List<Callable<Object>> result = new ArrayList<Callable<Object>>();

		for (String serviceId : serviceIds) {

			String viewId = getViewId(serviceId, input.view);

			if (viewId == null) {
				continue;
			}

			Collection<String> deployments = getDeployments(serviceId, dgInput);
			
			for (String deployment : deployments) {
				result.add(new GraphAsyncTask(serviceId, viewId, input.view, 
						getInput(dgInput, deployment), timeSpan,
						serviceIds, pointsWanted));
			}
		}

		return result;
	}
}
