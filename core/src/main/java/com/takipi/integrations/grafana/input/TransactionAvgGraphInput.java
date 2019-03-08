package com.takipi.integrations.grafana.input;

/**
 * A function returning a set of times series depicting the weighted avg response of calls into a target set 
 * of entry points (i.e. transactions)
 *
 * Example query:
 * 
 * 		transactionsAvgGraph({"graphType":"view","volumeType":"invocations","view":"$view",
 * 		"timeFilter":"$timeFilter","environments":"$environments", "applications":"$applications",
 * 		"deployments":"$deployments","servers":"$servers","aggregate":true,
 * 		"seriesName":"Avg Response (ms)","pointsWanted":"$pointsWanted","transactions":"$transactions"})
 * 
 * 	Screenshot: https://drive.google.com/file/d/1Hq-4iamqMxyDMRVb7nIQJVi1sW7R479Q/view?usp=sharing
 */
public class TransactionAvgGraphInput extends TransactionsGraphInput
{
	
}
