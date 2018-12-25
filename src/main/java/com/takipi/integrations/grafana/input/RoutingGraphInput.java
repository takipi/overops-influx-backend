package com.takipi.integrations.grafana.input;

/**
 * 
 * A function which returns time series for event volume grouped by tier. Key tiers
 * are returned first followed by tiers whose event volume is the highest.
 * 
 * Example query:
 * 		routingGraph({"graphType":"view","volumeType":"all","view":"$view",
 * 			"timeFilter":"$timeFilter","environments":"$environments", 
 * 			"applications":"$applications", "servers":"$servers", "deployments":"$deployments",
 * 			"pointsWanted":"$transactionPointsWanted","types":"$type",
 * 			"transactions":"$transactions", "limit":"$splitLimit"})
 * 
 * Screenshot: https://drive.google.com/file/d/1qBk94hF_3hPaAH-52rmX5FdbGrM-hVJD/view?usp=sharing

 */
public class RoutingGraphInput extends GraphLimitInput {
	
}
