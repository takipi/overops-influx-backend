package com.takipi.integrations.grafana.input;

/**
 * A function used to map the value of a template variable to provided constant. 
 * Used for internal purposes.
 *
 */
public class VariableRedirectInput extends VariableInput
{
	public String variable;
	public String dictionary;
}
