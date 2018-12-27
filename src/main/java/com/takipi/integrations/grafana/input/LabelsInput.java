package com.takipi.integrations.grafana.input;

/**
 * This function populates a variable containing all label names defined in the target environment(s)
 * matching an optional regex pattern.
 * 
 *  Example query:
 *  		labels({"environments":"$environments","sorted":true, "lablesRegex":"^infra"})
 *
 */
public class LabelsInput extends BaseEnvironmentsInput {
	
	/**
	 * An optional regex pattern used to include / exclude label names from being returned by 
	 * querying this function. For example, a value of "^infra" will only return labels whose 
	 * name starts with "infra".
	 */
	public String lablesRegex;
}
