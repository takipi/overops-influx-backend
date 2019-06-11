package com.takipi.integrations.grafana.input;

import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;

/**
 * A function used to return a single stat depicting the rate between calls into a target set of events entry points
 * and the event volume. This could be used to show the ratio between calls into events entry points (i.e. transaction throughput)
 * and a specific type of errors taking place within them (e.g transaction failures).
 * 
 * Example query:
 * 		transactionsRate({"type":"sum","volumeType":"invocations","view":"$view",
 * 		"timeFilter":"$timeFilter","environments":"$environments", "applications":"$applications",
 * 		"deployments":"$deployments","servers":"$servers","types":"$transactionFailureTypes",
 * 		"transactions":"$transactions", "filter":"events","searchText":"$searchText", 
 * 		"transactionSearchText":"$searchText","pointsWanted":"$transactionPointsWanted"})
 * 
 * 	Screenshot: https://drive.google.com/file/d/1__-49ejQq0TAiRZ2l7sJC16nZRzA7kQZ/view?usp=sharing
 */
public class TransactionsRateInput extends TransactionsVolumeInput {
	/**
	 * the event volume type which serves as the denominator between throughput and volume returned 
	 * by this function
	 */
	public VolumeType eventVolumeType;
	
	/**
	 * The type of events whose volume is use to include in the denominator
	 */
	public enum TransactionFilterType {
		
		/**
		 * include all events that are not Timers
		 */
		events, 
		
		/**
		 * include only events of type Timer. This can be used to produce the ration between
		 * throughput (i.e. calls into event entry points) and the number s of times timers
		 * set within them have exceeded their target thresholds.
		 */
		timers;
	}
	
	/**
	 * The types of events to include in the denominator of the function's return value
	 */
	public TransactionFilterType filter;
	
	/**
	 * whether to limit the rate to 100. The rate could be greater in case a logged error for example
	 * happens more than once per transaction as in the case of a retry loop.
	 */
	public boolean allowExcceed100;


}
