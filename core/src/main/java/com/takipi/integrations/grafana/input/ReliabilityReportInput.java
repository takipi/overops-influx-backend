package com.takipi.integrations.grafana.input;

import java.util.Arrays;
import java.util.List;

/** 
 * A function returning a report for a a target set of applications, deployments or tiers (i.e. categories)
 * listing an possible new errors, increases (i.e. regressions) and slowdowns. The function can return
 * either a tabular report or a chart, where a key selected value from each row is used as the Y value
 * and the name of the target app, deployment, tier is used as the X value. The decisions regarding
 * which events are new, regressing and which transactions have slowdowns are governed by the 
 * regression and transaction setting available in the Settings dashboard.
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
 * ˙
 *  Screenshot: https://drive.google.com/file/d/1aEXcfTGC9OfNaJvsEeRptqp2o1czd0SW/view?usp=sharing
 */
public class ReliabilityReportInput extends RegressionsInput {
	
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
		 * The report will return a row for each application in the environment with additional
		 * information about throughput and failure rates. The report
		 * will list up to <limit> apps, sorted by the apps who have the highest volume of events
		 */
		Apps_Extended, 
		
		
		/**
		 * The report will return a row for the last <limit> deployments in the target envs.
		 */
		Deployments, 
		
		/**
		 * The report will return a row for the last <limit> tiers in the target envs which 
		 * have the highest volume of events. If any key tiers are defiend in the Settings dashboard
		 * they are used first.
		 */
		Tiers,
		
		/**
		 * The report will return a a single row for the target event set
		 */
		Default;
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
		ErrorIncreases, 
		
		/**
		 * chart the number of severe increasing errors 
		 */
		SevereErrorIncreases,
		

		/**
		 * chart the number of transaction slowdowns
		 */
		Slowdowns, 
		
		/**
		 * chart the number of severe transaction slowdowns
		 */
		SevereSlowdowns,
		
		/**
		 * chart the volume of events of the target app, deployment, tier
		 */
		ErrorVolume,
		
		/**
		 * chart the unique number of events of the target app, deployment, tier
		 */
		UniqueErrors,
		
		/**
		 * chart the relative rate of events of the target app, deployment, tier
		 */
		ErrorRate,
		
		/**
		 * chart the reliability score of the target app, deployment, tier
		 */
		Score;
	}
	
	/**
	 * Control whether to report only on live deployment in Deployment report mode
	 */
	public boolean liveDeploymentsOnly;
	
	/**
	 * The specific value to chart
	 */
	public String graphType;
	
	/** 
	 * The target filters to report each row by: application, deployment or tier
	 */
	public ReportMode mode;
	
	public ReportMode getReportMode() {
		
		if (mode == null) {
			return  ReportMode.Default;
		}
		
		return mode;	
	}
	
	/**
	 * Control the type of events used to compute the report score
	 *
	 */
	public enum ScoreType {
		
		/**
		 * Include regression analysis for new events only
		 */
		NewOnly, 
		
		/**
		 * Include regression analysis for new and increasing events
		 */
		Regressions, 
		
		/**
		 * Include slowdown analysis 
		 */
		Slowdowns, 
	
		/**
		 * Combine regression and slowdown analysis in the output (default)
		 */
		Combined
	}
	
	public ScoreType scoreType;
	
	public ScoreType getScoreType() {
		
		if (scoreType == null) {
			return ScoreType.Combined;
		}
		
		return scoreType;
	}
	
	/**
	 * The max number of rows / time series points to return
	 */
	public int limit;
	
	/**
	 * Control whether non-key apps added to report are sorted by volume
	 */
	public boolean queryAppVolumes;
	
	
	/**
	 * A comma delimited pair of numeric values used to define thresholds by which
	 * to choose a postfix for a score series based on the values set in postfixes
	 */
	public String thresholds;
	
	/**
	 * A comma delimited arrays of 3 postfix to be added to the series name. The post fix
	 * is selected based on if the reliability score is smaller than, in between or greater than the upper
	 * threshold. For example if this is set to "BAD,OK,GOOD" and thresholds is "70,85"
	 * a score below 70 will have the postfix BAD added to the series name, 80 will be
	 * OK and 90 will be GOOD 
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
	
	public enum SortType {
		
		Ascending,
		
		Descending,
		
		Default
	}
	
	public SortType sortType;	
	
	public SortType getSortType() {
		
		if (sortType == null) {
			return SortType.Default;
		}
		
		return sortType;
	}
	
	/**
	 *  An optional comma delimited list of types to be used to calculate
	 *  failure types in extended app reporting mode. If no value if provided the types value
	 *  is used.
	 *  
	 */
	public String failureTypes;
	
	public String getFailureTypes() {
		
		if (failureTypes == null) {
			return types;
		}
		
		return failureTypes;
	}
	
	/**
	 * The diff reliability state calculated for an app in extended reporting mode
	 * this applies to slowdown, new, increasing and transaction fail rate states 
	 *
	 */
	public enum ReliabilityState {
		OK,
		Warning,
		Severe
	}
	
	/**
	 * the min transaction fail rate above which to calc the transction fail rate  
	 */
	public double minFailRate;
	
	public String statusPrefixes;
	
	public String scoreRanges;

	
	/**
	 * Below are the constants describing the field supported by this function for each of the available 
	 * report modes
	 */
	
	/**
	 * The field name  for the env ID for this row
	 */
	
	public static final String SERVICE = "Service";

	/**
	 * The field for the name of the target app/dep/tier
	 */
	public static final String KEY = "Key";

	/**
	 * The field name for the human-readable name  of the target app/dep/tier
	 */
	public static final String NAME = "Name";

	/**
	 * The field name of the row is a deployment, the name of the deployment to be diff against
	 */
	
	public static final String PREV_DEP_NAME = "previousDepName";
	
	/**
	 * The field name of the row is a deployment, the start of the deployment to be diff against
	 */
	public static final String PREV_DEP_FROM = "previousDepFrom";			

	/**
	* The field name of the row is a deployment and a diff deployment exists = 1, otherwise 0
	 */
	public static final String PREV_DEP_STATE =  "previousDepState";
	
	/**
	 * The field name of the row is a Value of any new issues in this row
	 */
	public static final String NEW_ISSUES = "NewIssues";
	
	/**
	 * The field name of the value of any regressions in this row
	 */
	public static final String REGRESSIONS = "Regressions";
	
	/**
	 * The field name of the value of any slowdowns in this row
	 */
	public static final String SLOWDOWNS = "Slowdowns"; 
	
	/**
	 * The field name of the Text description of new issues in this row
	 */
	public static final String NEW_ISSUES_DESC = "NewIssuesDesc";
	
	/**
	 * The field name of the Text description of regressions in this row
	 */
	public static final String REGRESSIONS_DESC = "RegressionsDesc";
	
	/**
	 * The field name of the Text description of slowdowns in this row
	 */
	public static final String SLOWDOWNS_DESC = "SlowdownsDesc";
	
	/**
	 * The field name of the Reliability score for this row
	 */
	public static final String SCORE = "Score"; 
	
	/**
	 * The field name of the Text description of reliability score for this row
	 */
	public static final String SCORE_DESC = "ScoreDesc";
	
	/**
	 * Field name of transaction volume for this row
	 */
	public static final String TRANSACTION_VOLUME = "TransactionVolume";


	/**
	 * Field name of transaction avg response for this row
	 */
	public static final String TRANSACTION_AVG_RESPONSE = "TransactionAvgResponse";

	/**
	 * Field name of transaction avg response delta for this row
	 */
	public static final String TRANSACTION_RESPONSE_DELTA = "TransactionResponseDelta";
	
	
	/**
	 * Field name of transaction failure volume
	 */
	public static final String TRANSACTION_FAILURES = "TransactionFailures";
	
	/**
	 * Field name of transaction failure rate for this row
	 */
	public static final String TRANSACTION_FAIL_RATE = "TransactionFailRate";
	
	/**
	 * Field name of transaction failure delta rate for this row
	 */
	public static final String TRANSACTION_FAIL_RATE_DELTA = "TransactionFailRateDelta";

	/**
	 * Field name of error volume for this row
	 */
	public static final String ERROR_VOLUME = "ErrorVolume";

	/**
	 * Field name of error count for this row
	 */
	public static final String ERROR_COUNT = "ErrorCount";
	
	/**
	 * Field name of overall reliability state.
	 */
	public static final String RELIABILITY_STATE = "ReliabilityState";
	
	/**
	 * Field name of transaction failure rate description
	 */
	public static final String TRANSACTION_FAIL_DESC = "FailureDesc";
	
	/**
	 * Field name of the an app reliability state description
	 */
	public static final String RELIABILITY_DESC = "RelabilityDesc";
	
	/**
	 * Field name of the an app reliability state description
	 */
	public static final String STATUS_NAME = "StatusName";
	
	

	/**
	 * The list of default fields returned for dep reporting
	 */
	public static final List<String> DEFAULT_DEP_FIELDS = Arrays.asList(
		new String[] { 	
			ViewInput.FROM,
			ViewInput.TO, 
			ViewInput.TIME_RANGE, 
			SERVICE, 
			KEY, 
			NEW_ISSUES_DESC, 
			REGRESSIONS_DESC, 
			SLOWDOWNS_DESC,
			SCORE_DESC,
			PREV_DEP_NAME, 
			PREV_DEP_FROM, 
			NAME,
			PREV_DEP_STATE,
			NEW_ISSUES, 
			REGRESSIONS, 
			SLOWDOWNS, 
			SCORE, 		
		});
	
	/**
	 * The list of default fields returned for app / tier reporting
	 */
	public static final List<String> DEFAULT_APP_FIELDS = Arrays.asList(
		new String[] { 	
			ViewInput.FROM, 
			ViewInput.TO, 
		 	ViewInput.TIME_RANGE, 
		 	SERVICE, 
		 	KEY, 
		 	NEW_ISSUES_DESC, 
		 	REGRESSIONS_DESC, 
			SLOWDOWNS_DESC,
		 	SCORE_DESC,
		 	NAME, 
			NEW_ISSUES, 
		  	REGRESSIONS, 
		 	SLOWDOWNS, 		 	
		 	SCORE
		});
	
	/**
	 * The list of default fields returned for extended app reporting
	 */
	public static final List<String> DEFAULT_APP_EXT_FIELDS = Arrays.asList(
		 new String[] { 	
			ViewInput.FROM, 
			ViewInput.TO, 
			ViewInput.TIME_RANGE, 
			SERVICE, 
			KEY, 
		 	RELIABILITY_STATE,
			NEW_ISSUES_DESC, 
			REGRESSIONS_DESC, 
			SLOWDOWNS_DESC,
			SCORE_DESC,	
			TRANSACTION_FAIL_DESC,
			RELIABILITY_DESC,
			NAME, 
			STATUS_NAME,
			TRANSACTION_VOLUME,
			TRANSACTION_AVG_RESPONSE,
		 	NEW_ISSUES, 
		  	REGRESSIONS, 
		 	SLOWDOWNS, 
		 	ERROR_VOLUME, 
		 	ERROR_COUNT,
			SCORE,
			TRANSACTION_FAIL_RATE, 
		 	TRANSACTION_FAILURES,
		 	TRANSACTION_FAIL_RATE_DELTA
		});
}
