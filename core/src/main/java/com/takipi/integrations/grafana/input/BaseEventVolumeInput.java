package com.takipi.integrations.grafana.input;

/**
 * The base input for all function operating on event volume data that requires
 * the specification of a graph point resolution from the Overops REST API.
 *
 */
public abstract class BaseEventVolumeInput extends EventFilterInput
{
	/**
	 * An int value used to specify the number of data points to pass
	 * when requesting graph data used to populate the event volume and stats (e.g. hits, invocations)
	 * of event data returned by this query. 
	 */
	public int pointsWanted;
	
}
