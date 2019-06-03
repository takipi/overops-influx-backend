package com.takipi.integrations.grafana.input;

/**
 * This function populates a template variable holding a list of transactions whose performance state matches that of
 * a target set of performance states. 
 *
 * Example query:
 * 
 * 		slowTransactions({"environments":"$environments","view":"All Events",
 * 		"timeFilter":"$timeFilter","sorted":"true", 
 * 		"pointsWanted":"$transactionPointsWanted", "performanceStates":"SLOWING|CRITICAL"})
 */
public class SlowTransactionsInput extends BaseEventVolumeInput {
	/**
	 * A | delimited array of performance states, that a target transaction must meet in order to be returned
	 * by this function. Possible values are: 	NO_DATA, OK, SLOWING, CRITICAL
	 */
	public String performanceStates;
}
