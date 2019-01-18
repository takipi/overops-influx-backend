package com.takipi.integrations.grafana.input;

/**
 * The base function input for all functions that are used to populate template variables:
 * http://docs.grafana.org/reference/templating/ 
 */
public abstract class VariableInput extends FunctionInput {
	
	 /** Control whether values of this variable are sorted alphabetically or logically */
	public boolean sorted;
	
	 /** Controls whether values are added as separate items, or as one comma delimited value */
	public boolean commaDelimited;
}
