package com.takipi.integrations.grafana.functions;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.VariableInput;

public class ApiHostFunction extends VariableFunction
{
	private static final String SAAS_API = "api.overops.com";
	private static final String SAAS_APP = "app.overops.com";
	
	private static final String HTTP = "http://";
	private static final String HTTPS = "https://";
	
	public ApiHostFunction(ApiClient apiClient)
	{
		super(apiClient);
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ApiHostFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return VariableInput.class;
		}

		@Override
		public String getName() {
			return "apiHost";
		}
	}

	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender)
	{
		String hostName = apiClient.getHostname().
			replaceAll(SAAS_API, SAAS_APP).replaceAll(HTTP, "").replaceAll(HTTPS, "");
		appender.append(hostName);
	}
}
