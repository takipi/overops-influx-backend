package com.takipi.integrations.grafana.input;


/**
 * The base for all functions that are used to populate template variables 
 *
 *
 */
public class VariableInput extends FunctionInput {
	
	 /** Control whether values of this variable are sorted alphabetically or logically */
	public boolean sorted;
}
