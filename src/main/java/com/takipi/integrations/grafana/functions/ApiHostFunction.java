package com.takipi.integrations.grafana.functions;

import java.net.MalformedURLException;
import java.net.URL;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.ApiHostInput;
import com.takipi.integrations.grafana.input.ApiHostInput.HostType;
import com.takipi.integrations.grafana.input.FunctionInput;

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
			return ApiHostInput.class;
		}

		@Override
		public String getName() {
			return "apiHost";
		}
	}
	
	private String cleanHost(String host) {
		return host.replaceAll(SAAS_API, SAAS_APP).replaceAll(HTTP, "").replaceAll(HTTPS, "");
	}

	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender)
	{
		ApiHostInput ahInput = (ApiHostInput)input;
		String hostName = apiClient.getHostname();
	
		try
		{
			HostType type;
			
			if (ahInput.type != null) {
				type = ahInput.type;
			} else {
				type = HostType.URL;
			}
			
			String value = null;
			
			switch (type) {
				
				case URL: 
					value = cleanHost(hostName);
					break;
					
				case URL_NO_PORT: 
					URL url = new URL(hostName);
					URL urlNoPort = new URL(url.getProtocol(), url.getHost(), -1, url.getFile(), null);
					String noPort = urlNoPort.toString();		
					value = cleanHost(noPort);
					break;
					
				case PORT:
					url = new URL(hostName);
					
					if (url.getPort() > 0) {
						value = String.valueOf(url.getPort());
					} else {
						value = String.valueOf(url.getDefaultPort());
					}
			}
					
			if (value != null) {
				appender.append(value);
			}
		}
		catch (MalformedURLException e)
		{
			throw new IllegalStateException(e);
		}
		
	}
}
