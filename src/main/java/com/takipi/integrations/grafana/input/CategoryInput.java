package com.takipi.integrations.grafana.input;

/**
 *	This function returns Graph series for each of the views held within a target category
 *	Example query:
 *		graph({"graphType":"view","volumeType":"all","view":"$view",
 *		"timeFilter":"$timeFilter","environments":"$environments", "applications":"$applications",
 *		"servers":"$servers", "deployments":"$deployments","pointsWanted":"$pointsWanted",
 *		"types":"$type",seriesName":"Times", "transactions":"$transactions", 
 *		"searchText":"$search", "category":"Apps", "limit":5})

 */
public class CategoryInput extends GraphInput {
	
	/**
	 * The category name from which to produce graphs
	 */
	public String category;
	
	/**
	 * Limit the number of graphs returned by this function. The views chosen by the function
	 * are those with the most volume of events within the category. For example, by setting this
	 * value to 3, the 3 views with the most volume of events within the category will be returned. 
	 */
	public int limit;
}
