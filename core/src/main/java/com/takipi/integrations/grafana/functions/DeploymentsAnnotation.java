package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.deployment.SummarizedDeployment;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.DeploymentsGraphInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.DeploymentUtil;
import com.takipi.integrations.grafana.util.TimeUtil;

public class DeploymentsAnnotation extends BaseGraphFunction {
		
	private static final String DEPLOY_SERIES_NAME = "deployments";
	private static final int MAX_DEPLOY_ANNOTATIONS = 5;

	protected class DeploymentData {
		protected String name;
		protected String description;
		protected DateTime firstSeen;
		protected DateTime lastSeen;
	}
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new DeploymentsAnnotation(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return DeploymentsGraphInput.class;
		}

		@Override
		public String getName() {
			return "deploymentsAnnotation";
		}
	}

	public DeploymentsAnnotation(ApiClient apiClient) {
		super(apiClient);
	}
	
	private void sortDeployments(List<DeploymentData> deployments) {
		
		deployments.sort(new Comparator<DeploymentData>() {
				
			@Override
			public int compare(DeploymentData o1, DeploymentData o2) {
				
				long delta = o2.firstSeen.getMillis() - o1.firstSeen.getMillis();
				
				if (delta > 0) {
					return 1;
				}
				
				if (delta < 0) {
					return -1;
				}
				
				return 0;
			}
		});		
	}

	
	private List<DeploymentData> getDeploymentData(Collection<String> selectedDeployments,
		String serviceId, boolean active) {
		
		Collection<SummarizedDeployment> deps = DeploymentUtil.getDeployments(apiClient, serviceId, active);

		if (deps == null) {
			return null;
		}
			
		 List<DeploymentData> result = new ArrayList<DeploymentData>();
		
		for (SummarizedDeployment dep : deps) {
			
			if (dep.first_seen == null) {
				continue;
			}
			
			if ((!CollectionUtil.safeIsEmpty(selectedDeployments)) && 
				(!selectedDeployments.contains(dep.name))) {
				continue;	
			}
			
			DeploymentData deploymentData = new DeploymentData();
			deploymentData.name = dep.name;
			
			deploymentData.firstSeen = TimeUtil.getDateTime(dep.first_seen);
			deploymentData.lastSeen = TimeUtil.getDateTime(dep.last_seen);

			StringBuilder value = new StringBuilder();
						
			value.append(dep.name);
			value.append(": introduced ");
			value.append(prettyTime.format(deploymentData.firstSeen.toDate()));
			
			if (deploymentData.lastSeen != null) {
				value.append(", last seen ");
				value.append(prettyTime.format(deploymentData.lastSeen.toDate()));
			}
			
			deploymentData.description = value.toString();
			
			result.add(deploymentData);		
		}
		
		return result;
	}
	
	@Override
	protected List<GraphSeries> processServiceGraph(Collection<String> serviceIds, String serviceId, String viewId, String viewName,
			BaseGraphInput input, Pair<DateTime, DateTime> timeSpan, Object tag) {
			
		DeploymentsGraphInput dgInput = (DeploymentsGraphInput)input;
		
		String name = getServiceValue(DEPLOY_SERIES_NAME, serviceId, serviceIds);		
		Series series = createGraphSeries(name, 0);

		Collection<String> selectedDeployments = input.getDeployments(serviceId, apiClient);

		List<DeploymentData> deployments = getDeploymentData(selectedDeployments, serviceId, false);
		
		if (deployments == null) {
			return Collections.emptyList();
		}
		
		sortDeployments(deployments);
		
		int maxDeployments;
		
		if (!CollectionUtil.safeIsEmpty(selectedDeployments)) {
			maxDeployments = deployments.size();
		} else {
			maxDeployments = Math.min(deployments.size(),
				Math.max(MAX_DEPLOY_ANNOTATIONS, dgInput.limit));
		}
		
		String json = gson.toJson(input);
		Pair<Gson, String> gsonPair = Pair.of(gson, json);
		
		for (DeploymentData deploymentData : deployments) {
						
			if ((deploymentData.lastSeen != null) 
			 && (deploymentData.lastSeen.isBefore(timeSpan.getFirst()))) {
				continue;
			}
			
			if ((input.hasApplications()) 
			&& (!appHasDeployVolume(serviceId, gsonPair, timeSpan, deploymentData.name))) {
				continue;
			}
			
			Object timeValue = getTimeValue(deploymentData.firstSeen.getMillis(), input);
			
			series.values.add(Arrays.asList(new Object[] {timeValue, 
					getServiceValue(deploymentData.description, serviceId, serviceIds) }));
			
			if (series.values.size() > maxDeployments) {
				break;
			}
		}
		
		return Collections.singletonList(GraphSeries.of(series, 1, name));
	}
}
