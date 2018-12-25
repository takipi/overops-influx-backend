package com.takipi.integrations.grafana.input;

/** 
 * A function returning a report for a a target set of applications, deployments or tiers (i.e. categories)
 * listing an possible new errors, increases (i.e. regressions) and slowdowns. The function can return
 * either a tabular report or a chart, where a key selected value from each row is used as the Y value
 * and the name of the target app, deployment, tier is used as the X value. The decisions regarding
 * which events are new, regressing and which transactions have slowdowns are governed by the 
 * regression algorithm - XXX and transaction slowdown algorithm XXX 
 *
 * Example query for table:
 * 		regressionReport({"timeFilter":"$timeFilter","environments":"$environments",
 * 		"applications":"$applications","servers":"$servers","deployments":"$deployments",
 * 		"view":"$view","pointsWanted":"$pointsWanted", "transactionPointsWanted":"$transactionPointsWanted",
 * 		"types":"$type", "render":"Grid", "mode":"Tiers", "limit":"$limit", 
 * 		"sevAndNonSevFormat":"%d  (%d p1)", "sevOnlyFormat":"%d p1"})
 * 
 * Example query for graph:
 * 
 * 		regressionReport({"timeFilter":"$timeFilter","environments":"$environments",
 * 		"applications":"$applications","servers":"$servers","deployments":"$deployments",
 * 		"view":"$view","pointsWanted":"$pointsWanted", "transactionPointsWanted":"$transactionPointsWanted",
 * 		"types":"$type", "render":"Graph", "mode":"Deployments", "limit":"$limit",
 * 		"graphType":"$graphType"})
 * 
 *  Screenshot: https://drive.google.com/file/d/1aEXcfTGC9OfNaJvsEeRptqp2o1czd0SW/view?usp=sharing
 */
public class RelabilityReportInput extends RegressionsInput {
	
	/**
	 * Control the set of events and filters on which to report
	 *
	 */
	public enum ReportMode {
		/**
		 * The report will return a row for each application in the environment, the report
		 * will list up to <limit> apps, sorted by the apps who have the highest volume of events
		 */
		Applications, 
		
		/**
		 * The report will return a row for the last <limit> deployments in the target envs.
		 */
		Deployments, 
		
		/**
		 * The report will return a row for the last <limit> tiers in the target envs which 
		 * have the highest volume of events. If any key tiers are defiend in the Settings dashboard
		 * they are used first.
		 */
		Tiers;
	}
	
	/**
	 * The key value to be be used as the Y value of each point if the function is to return a graph 
	 */
	public enum GraphType {
		/**
		 * chart the number of new issues
		 */
		NewIssues, 
		
		/**
		 * chart the number of severe new issues
		 */
		SevereNewIssues, 
		
		/**
		 * chart the number of increasing errors 
		 */
		Regressions, 
		
		/**
		 * chart the number of severe increasing errors 
		 */
		SevereRegressions,
		

		/**
		 * chart the number of transaction slowdowns
		 */
		Slowdowns, 
		
		/**
		 * chart the number of severe transaction slowdowns
		 */
		SevereSlowdowns,
		
		/**
		 * chart the reliability score of the target app, deployment, tier
		 */
		Score;
	}
	
	/**
	 * The specific value to chart
	 */
	public String graphType;
	
	/** 
	 * The target filters to report each row by: application, deployment or tier
	 */
	public ReportMode mode;
	
	/**
	 * The max number of rows / time series points to return
	 */
	public int limit;
	
	/**
	 * Obsolete
	 */
	public String thresholds;
	
	/**
	 * Obsolete
	 */
	public String postfixes;
	
	/** 
	 * The number of graph points to retrieve per time transaction time series when calculating 
	 * slowdowns
	 */
	public int transactionPointsWanted;
	
	/**
	 * The string format used to present a row value containing both severe and non severe items
	 * will receive the number of issues and the number of severe issues as string format %d parameters,
	 * for example: "%d  (%d p1)"
	 */
	public String sevAndNonSevFormat;
	
	/**
	 * The string format used to present a row value containing non severe items
	 * will receive the number of issues as string format %d parameter,
	 * for example: "%d issues"
	 */
	public String sevOnlyFormat;
}
