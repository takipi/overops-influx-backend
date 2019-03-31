package com.takipi.integrations.grafana.settings;

import com.takipi.integrations.grafana.storage.KeyValueStorage;

public interface SettingsStorage extends KeyValueStorage {
	public String getDefaultServiceSettings();
}
