package com.takipi.integrations.grafana.input;

/**
 * 
	A function the returns a table containing the event diffs 
	between two groups of App/Dep/Server filters

	Example query:
	
	eventsDiff({"fields":"link,type,entry_point,introduced_by,jira_issue_url,
		id,rate_desc,diff_desc,diff,message,error_location,stats.hits,rate,first_seen,
		jira_state","view":"$view","timeFilter":"$timeFilter","environments":"$environments",
		"applications":"$applications","servers":"$servers","deployments":"$deployments",
		"volumeType":"all","maxColumnLength":80, "types":"$type",
		"pointsWanted":"$pointsWanted","transactions":"$transactions", 
		"searchText":"$search", "compareToApplications":"$compareToApplications", 
		"compareToDeployments":"$compareToDeployments","compareToServers":"$compareToServers", 
		"diffTypes":"Increasing"})
 *
 */
public class TransactionsDiffInput extends TransactionsListInput {
	/**
	 * A comma delimited array of application names to compare against
	 */
	public String baselineApplications;
	
	/**
	 * A comma delimited array of server names to compare against
	 */
	
	public String baselineServers;
	
	/**
	 * A comma delimited array of deployment names  to compare against
	 */
	public String baselineDeployments;
	
	/**
	 * Additional available field describing the diff for the current even from the prev release.
	 */ 
}
