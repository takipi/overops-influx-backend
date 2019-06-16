package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.takipi.api.client.ApiClient;
import com.takipi.common.util.CollectionUtil;
import com.takipi.integrations.grafana.input.CorrelateGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.SystemMetricsGraphInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.ServiceSettings;

public class CorrelateGraphFunction extends GrafanaFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new CorrelateGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return CorrelateGraphInput.class;
		}

		@Override
		public String getName() {
			return "correlateGraph";
		}
	}
	
	public CorrelateGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	public CorrelateGraphFunction(ApiClient apiClient, Map<String, ServiceSettings> settingsMaps) {
		super(apiClient, settingsMaps);
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof CorrelateGraphInput)) {
			throw new IllegalArgumentException("CorrelateGraphInput");
		}

		CorrelateGraphInput input = (CorrelateGraphInput) functionInput;
		
		Collection<String> metricNames = input.getMetricNames();
		
		if (CollectionUtil.safeIsEmpty(metricNames)) {
			return Collections.emptyList();
		}
		
		List<Series> result = new ArrayList<Series>();
		
		boolean hasThroughput = metricNames.contains(CorrelateGraphInput.THROUGHPUT_METRIC.toLowerCase());
		
		if (hasThroughput) {
			TransactionsGraphFunction tgFunction = new TransactionsGraphFunction(apiClient, settingsMaps);
			result.addAll(tgFunction.process(input));
		}
		
		boolean hasSysMetrics = (((hasThroughput) && (metricNames.size() > 1))  
				|| (metricNames.size() > 0)) ;
		
		if (hasSysMetrics) {
			
			String json = gson.toJson(input);
			
			SystemMetricsGraphInput smInput = gson.fromJson(json, SystemMetricsGraphInput.class);
			smInput.seriesName =  null;

			SystemMetricsGraphFunction smFunction = new SystemMetricsGraphFunction(apiClient, settingsMaps);
			
			if (hasThroughput) {
				List<String> sysMetrics = new ArrayList<String>(metricNames);
				sysMetrics.remove(CorrelateGraphInput.THROUGHPUT_METRIC.toLowerCase());
				smInput.metricNames = String.join(GrafanaFunction.GRAFANA_SEPERATOR_RAW, sysMetrics);
			}
			
			result.addAll(smFunction.process(smInput));
		}
		
		return result;
	}
}
