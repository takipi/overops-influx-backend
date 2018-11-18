package com.takipi.integrations.grafana.settings;

public interface SettingsStorage {

	public String getDefaultServiceSettings();
	public String getServiceSettings(String name);
	public void saveServiceSettings(String name, String settings);
}
