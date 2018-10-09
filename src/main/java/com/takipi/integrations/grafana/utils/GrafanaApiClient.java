package com.takipi.integrations.grafana.utils;

import com.takipi.common.api.ApiClient;

public class GrafanaApiClient {
	public static ApiClient getApiClient(String hostname, String token) {
			return ApiClient.newBuilder()
					.setHostname(hostname)
					.setApiKey(token)
					.build();
	}
}
