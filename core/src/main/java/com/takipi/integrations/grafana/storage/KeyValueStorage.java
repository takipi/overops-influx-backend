package com.takipi.integrations.grafana.storage;

public interface KeyValueStorage {
	public String getValue(String name);
	public void setValue(String name, String value);
}
