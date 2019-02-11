package com.takipi.integrations.grafana.input;


/**
 * A function returning a list of transaction available for a user to filter target events by.
 * If any key transaction groups are defined in the Settings dashboard are defined they are returned,
 * followed by a list of all transaction (entry point) names of events active within the provided time frame.
 * 
 * Example query:
 * 		transactions({"environments":"$environments","view":"All Events",
 * 		"timeFilter":"time >= now() - 7d","sorted":"true"})
 *
 * Screenshot: https://drive.google.com/open?id=1y_AneI6seWBF2GQxZDNXFYFB7Ku-0LBv
 */
public class TransactionsInput extends BaseEventVolumeInput {
	
}
