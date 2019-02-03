package com.takipi.integrations.grafana.input;

/**
 * A function used to describe the components of an event diff 
 */

public class EventsDiffDescInput extends EventsDiffInput
{
	/**
	 * 
	 *	Defines the description types available
	 */
	public enum DescType {
		
		/**
		 * describe group A filter
		 */
		FilterA,
		
		/**
		 * describe group A filter
		 */
		FilterB,
		
		/**
		 * describe both groups
		 */
		Combined
	}
	
	/**
	 * description type
	 */
	public DescType descType;
	
	public DescType getDescType() {
		
		if (descType == null) {
			return DescType.Combined;			
		}
		
		return descType;
	}
}
