package com.takipi.integrations.grafana.functions;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;

public abstract class EnvironmentVariableFunction extends VariableFunction {

	public EnvironmentVariableFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	protected abstract void populateServiceValues(EnvironmentsInput input, String[] serviceIds, String serviceId, VariableAppender appender); 
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {
		
		if (!(input instanceof EnvironmentsInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		EnvironmentsInput envInput = (EnvironmentsInput)input;
		
		String[] serviceIds = getServiceIds(envInput);
		
		for (String serviceId :serviceIds) {
			populateServiceValues(envInput, serviceIds, serviceId, appender);
		}
	}
}
