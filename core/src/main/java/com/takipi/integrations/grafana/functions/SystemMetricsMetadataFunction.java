package com.takipi.integrations.grafana.functions;

import java.util.Collection;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.functions.SystemMetricsMetadata.SystemMetric;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.SystemMetricsMetadataInput;

public class SystemMetricsMetadataFunction extends EnvironmentVariableFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new SystemMetricsMetadataFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return SystemMetricsMetadataInput.class;
		}
		
		@Override
		public String getName() {
			return "systemMetricsMetadata";
		}
	}
	
	public SystemMetricsMetadataFunction(ApiClient apiClient) {
		super(apiClient);
	}

	protected boolean getAddNone(SystemMetricsMetadataInput input) {
		return input.addNone;
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {

		if (!(input instanceof SystemMetricsMetadataInput)) {
			throw new IllegalArgumentException("input");
		}
		
		SystemMetricsMetadataInput smmInput = (SystemMetricsMetadataInput)input;
		
		if (getAddNone(smmInput)) {
			appender.append(NONE);
		}
		
		super.populateValues(input, appender);
	}
	
	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
		VariableAppender appender) {
		
		SystemMetricsMetadata metadata = SystemMetricsMetadata.of(apiClient, serviceId);
		
		for (SystemMetric systemMetric : metadata.metricList) {
			String value = getServiceValue(systemMetric.name, serviceId, serviceIds);
			appender.append(value);
		}
	}
}
