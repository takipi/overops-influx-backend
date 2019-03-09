package com.takipi.integrations.grafana.functions;

import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.service.SummarizedService;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;

public class EnvironmentsFunction extends VariableFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new EnvironmentsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EnvironmentsInput.class;
		}

		@Override
		public String getName() {
			return "environments";
		}
	}

	public EnvironmentsFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	@Override
	protected int compareValues(FunctionInput input, String o1, String o2)
	{
		if (NONE.equals(o1)) {
			return 1;
		}
		
		if (NONE.equals(o2)) {
			return 1;
		}
		
		return super.compareValues(input, o1, o2);
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {

		appender.append(NONE);
		
		List<SummarizedService> services = ClientUtil.getEnvironments(apiClient);

		for (SummarizedService service : services) {
			
			String cleanServiceName = service.name.replace(ARRAY_SEPERATOR_RAW, "").
				replace(GRAFANA_SEPERATOR_RAW, "");//replace(SERVICE_SEPERATOR_RAW, "");
			
			String value = getServiceValue(cleanServiceName, service.id);
			
			appender.append(value);
			
		}
	}
}
