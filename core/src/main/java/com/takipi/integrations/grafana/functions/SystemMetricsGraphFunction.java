package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.system.SystemMetricGraphPoint;
import com.takipi.api.client.request.metrics.system.SystemMetricGraphRequest;
import com.takipi.api.client.result.metrics.system.SystemMetricGraphResult;
import com.takipi.api.client.result.metrics.system.SystemMetricMetadataResult;
import com.takipi.api.client.util.validation.ValidationUtil.GraphResolution;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.SystemMetricsMetadata.SystemMetric;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.SystemMetricsGraphInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.ServiceSettings;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.TimeUtil;

public class SystemMetricsGraphFunction extends BaseGraphFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new SystemMetricsGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return SystemMetricsGraphInput.class;
		}

		@Override
		public String getName() {
			return "systemMetricsGraph";
		}
	}

	public SystemMetricsGraphFunction(ApiClient apiClient, Map<String, ServiceSettings> settingsMaps) {
		super(apiClient, settingsMaps);
	}
	
	public SystemMetricsGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	@Override
	protected Collection<Callable<Object>> getTasks(Collection<String> serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan) {
		
		SystemMetricsGraphInput smgInput = (SystemMetricsGraphInput)input;
		Collection<String> metrics = smgInput.getMetricNames();
		
		List<Callable<Object>> result = new ArrayList<Callable<Object>>();
		
		for (String serviceId : serviceIds) {

			Map<String, String> views = getViews(serviceId, input);

			for (Map.Entry<String, String> entry : views.entrySet()) {

				String viewId = entry.getKey();
				String viewName = entry.getValue();

				for (String metricName : metrics) {
					
					if (VAR_ALL.contains(metricName)) {
						continue;
					}
					
					result.add(new GraphAsyncTask(serviceId, viewId, viewName, input,
						timeSpan, serviceIds, metricName));
				}
			}
		}
		
		return result;	
	}
	
	private Collection<SystemMetricGraphResult> getSystemMetricGraphs(
		String serviceId,  SystemMetricsGraphInput input, 
		Pair<DateTime, DateTime> timeSpan, SystemMetric systemMetric) {
		
		List<SystemMetricGraphResult> result = new ArrayList<SystemMetricGraphResult>();
		
		Pair<String, String> fromTo = TimeUtil.toTimespan(timeSpan);
		GraphResolution graphResolution = getResolution(timeSpan);

		for (SystemMetricMetadataResult metadata : systemMetric.metadatas) {
		
			SystemMetricGraphRequest.Builder builder = SystemMetricGraphRequest.newBuilder()
				.setServiceId(serviceId).setFrom(fromTo.getFirst()).setTo(fromTo.getSecond())
				.setResolution(graphResolution).setMetricName(metadata.name);	
				
			applyFilters(input, serviceId, builder);
			
			Response<SystemMetricGraphResult> response = ApiCache.getSystemMetricsGraph(apiClient, 
				serviceId, input, getSettingsData(serviceId), builder.build(), metadata.name);
			
			if ((response.isBadResponse()) || (response.data == null) 
			|| (response.data.points == null)) {
				continue;
			}
			
			result.add(response.data);
		}
		
		return result;
	}
	
	@Override
	protected List<GraphSeries> processServiceGraph(Collection<String> serviceIds, String serviceId, String viewId,
		String viewName, BaseGraphInput input,
		Pair<DateTime, DateTime> timeSpan, Object tag) {
		
		SystemMetricsGraphInput smgInput = (SystemMetricsGraphInput)input;

		SystemMetricsMetadata metadata = SystemMetricsMetadata.of(apiClient, serviceId);
		SystemMetric systemMetric = metadata.metricMap.get(tag.toString());
		
		Collection<SystemMetricGraphResult> graphs = getSystemMetricGraphs(serviceId, 
				smgInput, timeSpan, systemMetric);
		
		Map<Long, double[]> values = new TreeMap<Long, double[]>();

		int index = 0;
		
		for (SystemMetricGraphResult graph : graphs) {
			
			for (SystemMetricGraphPoint gp : graph.points) {
				
				DateTime gpTime =  TimeUtil.getDateTime(gp.time);
				Long timeValue = Long.valueOf(gpTime.getMillis());
				
				double[] pointValues = values.get(timeValue);
				
				if (pointValues == null) {
					pointValues = new double[graphs.size()];
					values.put(timeValue, pointValues);
				}
				
				pointValues[index] = gp.value;				
			}
			
			index++;
		}
		
		List<List<Object>> seriesValues = new ArrayList<List<Object>>(values.size());

		for (Map.Entry<Long, double[]> entry : values.entrySet()) {
			double value = systemMetric.getValue(entry.getValue());
			seriesValues.add(Arrays.asList(new Object[] { entry.getKey(), value }));

		}
		
		StringBuilder seriesName = new StringBuilder();
		seriesName.append(systemMetric.name);
		
		String unit = systemMetric.getUnit();
		
		if ((unit != null) 
		&& (!GrafanaFunction.NONE.toLowerCase().equals(unit.toLowerCase())))  {
			seriesName.append("(");
			seriesName.append(unit.toLowerCase());
			seriesName.append(")");
		}
		
		seriesName.append("*");
		
		String tagName = getSeriesName(input, seriesName.toString(), serviceId, serviceIds);		
		String cleanTagName = cleanSeriesName(tagName);
		
		Series series = createGraphSeries(cleanTagName, 0, seriesValues);

		return Collections.singletonList(GraphSeries.of(series, 0, cleanTagName));		
	}
}
