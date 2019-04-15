package com.takipi.integrations.grafana.functions;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.CorrelateGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.SystemMetricsMetadataInput;

public class CorrelateGraphMetadataFunction extends SystemMetricsMetadataFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new CorrelateGraphMetadataFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return SystemMetricsMetadataInput.class;
		}
		
		@Override
		public String getName() {
			return "correlateMetadata";
		}
	}
	
	public CorrelateGraphMetadataFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {
		
		appender.append(CorrelateGraphInput.THROUGHPUT_METRIC);		
		super.populateValues(input, appender);
	}
	
	@Override
	protected boolean getAddNone(SystemMetricsMetadataInput input) {
		return false;
	}
	
}
