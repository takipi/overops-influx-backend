package com.takipi.integrations.grafana.settings;

import com.takipi.api.client.ApiClient;

public interface SettingsStorage {

	public String getDefaultServiceSettings();
	public String getServiceSettings(ApiClient apiClient, String name);
	public void saveServiceSettings(ApiClient apiClient, String name, String settings);
}
