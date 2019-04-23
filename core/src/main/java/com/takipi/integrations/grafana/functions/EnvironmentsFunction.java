package com.takipi.integrations.grafana.functions;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.service.SummarizedService;
import com.takipi.api.client.result.service.ServicesResult;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.util.ApiCache;

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
	protected int compareValues(FunctionInput input, String o1, String o2) {
		
		boolean o2None = NONE.equals(o2);
		boolean o1None = NONE.equals(o1);

		if (o2None) {
			return 1;
		}
		
		if (o1None) {
			return 1;
		}

		return super.compareValues(input, o1, o2);
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {

		appender.append(NONE);
		
		Response<ServicesResult> response = ApiCache.getServices(apiClient);

		if ((response == null) || (response.isBadResponse()) 
		|| (response.data == null) || (response.data.services ==  null)) {
			return;
		}
		
		for (SummarizedService service : response.data.services) {
			
			String cleanServiceName = service.name.replace(ARRAY_SEPERATOR_RAW, "").
				replace(GRAFANA_SEPERATOR_RAW, "");//replace(SERVICE_SEPERATOR_RAW, "");
			
			String value = getServiceValue(cleanServiceName, service.id);
			
			appender.append(value);
			
		}
	}
}
