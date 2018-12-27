package com.takipi.integrations.grafana.input;

/**
 *	The base input for all graph function that limit the number of time series returned by the function.
 *	This could be effective, where a possible graph function can return a large number of series (for example
 *	a category graph that holds many views), and there is a need to limit based on certain criteria (e.g. which
 *	series has more volume) the number of actual graphs presented to the user.
 *
 */
public class GraphLimitInput extends GraphInput {
	
	/**
	 * The max number of time series to be returned by a query invoking this function.
	 */
	public int limit;
}
