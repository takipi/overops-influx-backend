package com.takipi.integrations.grafana.input;


/**
 * This query is used to populate a baseline graph annotation. This query will produce one point
 * on all graphs within the dashboard that specifies where the baseline window ends and the active window
 * begin for regression and slowdown calculations.
 * 
 * Example query:
 * 		baselineAnnotation({"graphType":"view","view":"$view",
 * 		"timeFilter":"time >= now() - $timeRange","environments":"$environments", 
 * 		"applications":"$applications", "servers":"$servers","deployments":"$deployments",
 * 		"text":"<- baseline %s | active window %s  ->"})
 */
public class BaselineAnnotationInput extends GraphInput
{
	/**
	 * This value is used to String format the annotation text. It should be passed a String format
	 * which will be passed a string format %s value representing the baseline window value (e.g. 14d)
	 * and a second %s which will hold the value of the active window (e.g. 1d).
	 */
	public String text;
}
