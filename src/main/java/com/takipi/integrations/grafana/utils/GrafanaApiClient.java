package com.takipi.integrations.grafana.utils;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.servlet.ServletUtil.Auth;

public class GrafanaApiClient {

	public static ApiClient getApiClient(Auth auth) {
	
		String host;
		String token;
		
		if (auth != null) {
			host = auth.hostname;
			token = auth.token;
		} else {
			String authProp = System.getProperty("auth");
			
			if (authProp == null) {
				throw new IllegalArgumentException();
			}
			
			int index = authProp.indexOf('=');
			
			if (index == -1) {
				throw new IllegalArgumentException(authProp);
			}
			
			host = authProp.substring(0, index);
			token = authProp.substring(index + 1, authProp.length());
		}
		
		return getApiClient(host, token);
	}

	public static ApiClient getApiClient(String hostname, String token) {
		return ApiClient.newBuilder().setHostname(hostname).setApiKey(token).build();
	}
}
