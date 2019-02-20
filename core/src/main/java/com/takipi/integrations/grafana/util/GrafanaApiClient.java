package com.takipi.integrations.grafana.util;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.servlet.ServletUtil.Auth;

public class GrafanaApiClient {

	private static final int TIMEOUT = 120000;

	public static ApiClient getApiClient() {
		return getApiClient(null); 
	}
	
	public static ApiClient getApiClient(Auth auth) {
		String host;
		String token;

		if (auth != null) {
			host = auth.hostname;
			token = auth.token;
		} else {
			String authProp = System.getProperty("auth");

			if (authProp == null) {
				host = "null";
				token = "null";
			} else {

				int index = authProp.indexOf('=');
	
				if (index == -1) {
					throw new IllegalArgumentException(authProp);
				}
	
				host = authProp.substring(0, index);
				token = authProp.substring(index + 1, authProp.length());
			}
		}

		return getApiClient(host, token);
	}

	public static ApiClient getApiClient(String hostname, String token) {
		return ApiClient.newBuilder()
            .setHostname(hostname)
            .setApiKey(token)
            .setConnectTimeout(TIMEOUT)
            .build();
	}
}
