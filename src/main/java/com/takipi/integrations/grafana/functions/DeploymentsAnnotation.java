package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

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
		
		Collection<SummarizedDeployment> deps = DeploymentUtil.getDeployments(apiClient, serviceId, false);
		
		if (deps == null) {
			return Collections.emptyList();
		}
		
		List<Pair<Long, String>> deployments = new ArrayList<Pair<Long, String>>();
		
		for (SummarizedDeployment dep : deps) {
			
			if (dep.first_seen == null) {
				continue;
			}
			
			DateTime firstSeen = ISODateTimeFormat.dateTime().withZoneUTC().parseDateTime(dep.first_seen);
			deployments.add(Pair.of(firstSeen.getMillis(), dep.name));
			
		}
		
		deployments.sort(new Comparator<Pair<Long, String>>() {
			@Override
			public int compare(Pair<Long, String> o1, Pair<Long, String> o2)
			{
				return (int)(o2.getFirst().longValue() - o1.getFirst().longValue());
			}
		});
		
		int maxDeployments = Math.min(deployments.size(), MAX_DEPLOY_ANNOTATIONS);
		
		for (int i = 0; i < maxDeployments; i++) {
			Pair<Long, String> pair = deployments.get(i);
			
			Object timeValue = getTimeValue(pair.getFirst(), input);
			
			result.series.values.add(Arrays.asList(new Object[] {timeValue, 
					getServiceValue(pair.getSecond(), serviceId, serviceIds) }));
		}
		
		return Collections.singletonList(result);
	}
}
