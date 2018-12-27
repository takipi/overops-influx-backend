package com.takipi.integrations.grafana.input;

/**
 * This function populates a template variable containing a views within an optional category. 
 *	
 * Example query:
 * 		views({"environments":"$environments", "sorted":true, "category":"Tiers"})	
 *
 */
public abstract class ViewsInput extends BaseEnvironmentsInput {
	
	/**
	 * An optional category name containing the views to return. If no value is provided all
	 * views within the selected environments are returned.
	 */
	public String category;
}
