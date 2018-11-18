package com.takipi.integrations.grafana.settings;

import com.takipi.api.client.ApiClient;

public interface SettingsStorage {

	public String getDefaultServiceSettings();
	public String getServiceSettings(String name);
	public void saveServiceSettings(String name, String settings);
}
