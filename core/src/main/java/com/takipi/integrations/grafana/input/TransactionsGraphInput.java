package com.takipi.integrations.grafana.input;

/**
 * A function returning a set of times series depicting the volume of calls into a target set 
 * of entry points (i.e. transactions)
 *
 * Example query:
 * 
 * 		transactionsGraph({"graphType":"view","volumeType":"invocations","view":"$view",
 * 		"timeFilter":"$timeFilter","environments":"$environments", "applications":"$applications",
 * 		"deployments":"$deployments","servers":"$servers","aggregate":true,
 * 		"seriesName":"Throughput","pointsWanted":"$pointsWanted","transactions":"$transactions"})
 * 
 * 	Screenshot: https://drive.google.com/file/d/1i_9DjK-mugjsagBKh-ZuJb3G07AtcyH6/view?usp=sharing
 */
public class TransactionsGraphInput extends BaseGraphInput {
	
	/**
	 * Control the Y value of the return time series data.
	 *
	 */
	public enum GraphType {
		/**
		 * use the avg response time to complete calls into the target transactions as the Y value
		 */
		avg_time, 
		
		/**
		 * use the number of calls into the target transactions as the Y value
		 */
		invocations, 
		
		/**
		 * return time series for the number of calls AND the avg response time to complete calls into the target transactions as the Y value
		 */
		all
	}
	
	/**
	 * The volume type to be used for the Y value of each of the points in the time series.
	 */
	public GraphType volumeType;
	
	public enum AggregateMode {
		
		/**
		 * aggregate the selected transactions into one series
		 */
		Yes,
		
		/**
		 * split the selected transactions into multiple series
		 */
		No,
		
		/**
		 * aggregate the selected transactions into one series
		 * if the selection is a group or top transaction filter, otherwise split
		 */
		Auto
	}
	
	public AggregateMode getAggregateMode() {
		
		if (aggregateMode == null) {
			return AggregateMode.Auto;
		}
		
		return aggregateMode;
	}
		
	
	/**
	 * Controls whether the time series Y values for the matching transactions are merged into a single
	 * aggregate series.
	 */
	public AggregateMode aggregateMode;
	
	/**
	 * Control the max number of separate time series returned by this function if aggregate is set to false
	 */
	public int limit;
	
	/**
	 * A comma delimited array of performance states, that a target transaction must meet in order to be returned
	 * by this function. Possible values are: 	NO_DATA, OK, SLOWING, CRITICAL
	 */
	public String performanceStates;
	
	/**
	 * 
	 * Control which time window is used for Y values
	 */
	public enum TimeWindow {
		/**
		 * Use the avg response of the active time window as the Y value
		 */
		Active, 
		
		/**
		 * Use the avg response of the baseline time window as the Y value
		 */
		Baseline, 
		
		/**
		 * Add the avg response of the active and baseline time windows as the Y value
		 */
		All
	}
	
	/**
	 * Control which time window's points to use to calculate the weighted avg. Default = All
	 */
	public TimeWindow timeWindow;
	
	public TimeWindow getTimeWindow() {
		
		TimeWindow result = this.timeWindow;
		
		if (result == null) {
			result = TimeWindow.All;
		}
		
		return result;
	}
	
	public String timeFilterVar;
}
