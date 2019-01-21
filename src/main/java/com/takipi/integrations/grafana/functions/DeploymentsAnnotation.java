package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.ocpsoft.prettytime.PrettyTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.deployment.SummarizedDeployment;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.DeploymentsGraphInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.DeploymentUtil;

public class DeploymentsAnnotation extends BaseGraphFunction {
		
	private static final String DEPLOY_SERIES_NAME = "deployments";
	private static final int MAX_DEPLOY_ANNOTATIONS = 3;
	
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
	
	private void sortDeployments(List<Pair<DateTime, String>> deployments) {
		
		deployments.sort(new Comparator<Pair<DateTime, String>>() {
				
			@Override
			public int compare(Pair<DateTime, String> o1, Pair<DateTime, String> o2) {
				
				long delta = o2.getFirst().getMillis() - o1.getFirst().getMillis();
				
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

	
	private void addDeployments(List<Pair<DateTime, String>> deployments, String serviceId, boolean active) {
		
		Collection<SummarizedDeployment> deps = DeploymentUtil.getDeployments(apiClient, serviceId, active);

		if (deps == null) {
			return;
		}
		
		PrettyTime prettyTime = new PrettyTime();
			
		for (SummarizedDeployment dep : deps) {
			
			if (dep.first_seen == null) {
				continue;
			}
			
			DateTime firstSeen = ISODateTimeFormat.dateTime().withZoneUTC().parseDateTime(dep.first_seen);
			StringBuilder value = new StringBuilder();
			
			value.append(dep.name);
			value.append(": introduced ");
			value.append(prettyTime.format(firstSeen.toDate()));
			
			if (dep.last_seen != null) {
				DateTime lastSeen = ISODateTimeFormat.dateTime().withZoneUTC().parseDateTime(dep.last_seen);
				value.append(", last seen ");
				value.append(prettyTime.format(lastSeen.toDate()));
			}
			
			deployments.add(Pair.of(firstSeen, value.toString()));		
		}
	}
	
	@Override
	protected List<GraphSeries> processServiceGraph(Collection<String> serviceIds, String serviceId, String viewId, String viewName,
			BaseGraphInput input, Pair<DateTime, DateTime> timeSpan, int pointsWanted) {
			
		GraphSeries result = new GraphSeries();
		
		result.series = new Series();
		
		result.series.name = EMPTY_NAME;
		result.series.columns = Arrays.asList(new String[] { TIME_COLUMN, 
			getServiceValue(DEPLOY_SERIES_NAME, serviceId, serviceIds) });
		
		result.series.values = new ArrayList<List<Object>>();
		result.volume = 1;
		
		List<Pair<DateTime, String>> deployments = new ArrayList<Pair<DateTime, String>>();
		
		addDeployments(deployments, serviceId, false);
		sortDeployments(deployments);
		
		int maxDeployments = Math.min(deployments.size(), MAX_DEPLOY_ANNOTATIONS);
		
		for (int i = 0; i < maxDeployments; i++) {
			Pair<DateTime, String> pair = deployments.get(i);
			
			Object timeValue = getTimeValue(pair.getFirst().getMillis(), input);
			
			result.series.values.add(Arrays.asList(new Object[] {timeValue, 
					getServiceValue(pair.getSecond(), serviceId, serviceIds) }));
		}
		
		return Collections.singletonList(result);
	}
}
