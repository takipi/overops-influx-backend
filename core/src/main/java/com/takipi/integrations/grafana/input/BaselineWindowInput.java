package com.takipi.integrations.grafana.input;

/**
 * This query is used to populate a variable holding the value of the current baseline window. 
 * This query will produce one value that can be used for both presentation purposes inside
 * widget titles as well as to set and Time window overrides for widgets which are used
 * to display graphs that show any regression and slowdown calculations.
 * 
 * Example query:
 * 		baselineWindow({"graphType":"view","view":"$view",
 * 		"timeFilter":"timeFilter","environments":"$environments",
 * 		"applications":"$applications", "servers":"$servers","deployments":"$deployments",
 * 		"windowType":"Active"})
 */

public class BaselineWindowInput extends BaseEventVolumeInput {
	/**
	 * 
	 * The type of window to return
	 */
	public enum WindowType {
		
		/**
		 * Return the active time window
		 */
		Active,
		
		/**
		 * Return the baseline time window
		 */
		Baseline,
		
		/**
		 * Return the active and baseline window combined
		 */
		Combined
	}
	
	/**
	 * Control whether output is returned in minute format (e.g. "90m"), 
	 * or if true in human readable format (e.g. "An hour an half ago").
	 */
	public boolean prettyFormat;
	
	/**
	 * Set whether to return the combined value of the active, baseline window,
	 * or their combined time span. Default is Active.
	 */
	public WindowType windowType;
	
	public WindowType getWindowType() {
		
		if (windowType == null) {
			return WindowType.Active;
		}
		
		return windowType;
	}
}
