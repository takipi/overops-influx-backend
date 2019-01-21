package com.takipi.integrations.grafana.input;

import java.util.Collection;

/**
 * Used for internal purposes to concatenate variable multiple selection into the
 * URL of dashboard opened from a table link
 *
 */
public class LimitVariableInput extends VariableInput
{
	public String name;
	public String values;
	
	public Collection<String> getValues() {
		return getServiceFilters(values, null, true);
	}
}
