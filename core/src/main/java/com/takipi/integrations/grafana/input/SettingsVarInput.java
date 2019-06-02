package com.takipi.integrations.grafana.input;

/**
 * Used for internal purposes to retrieve a value from the Settings dashboard. 
 *
 */
public class SettingsVarInput extends BaseEnvironmentsInput {
	public String name;
	public String defaultValue;
	public boolean convertToArray;
}
