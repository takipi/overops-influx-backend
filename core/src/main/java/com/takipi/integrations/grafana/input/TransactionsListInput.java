package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.takipi.api.client.util.performance.calc.PerformanceState;
import com.takipi.integrations.grafana.functions.GrafanaFunction;

/**
 * A function returning a list of rows depicting the volume and rates of calls into 
 * event entry points (i.e. transactions). This function can return a list of objects used to
 * populate table, or a single stat value used to populate a single stat widget.
 *
 * Example query:
 * 
 * 		transactionsList({"view":"$view", "timeFilter":"$timeFilter",
 * 		"environments":"$environments","applications":"$applications","servers":"$servers", "deployments":"$deployments", 
 * 		"fields":"link,slow_delta,delta_description,from,to,timeRange,
 * 		baseline_calls,active_calls,transaction,invocations,avg_response,baseline_avg,error_rate,errors",
 * 		"renderMode":"Grid","types":"$transactionFailureTypes","transactions":"$transactions", 
 * 		"searchText":"$searchText", "pointsWanted":"$transactionPointsWanted"})
 * 
 * 		Screenshot: 
 * 			https://drive.google.com/file/d/1KFy__D3nhGi8HLvrKkiLfwT8YgIUmaN1/view?usp=sharing
 *
 * 			https://drive.google.com/file/d/1A7eGLJIXvfUMHgvAcGEiY8uGTN1Xcn8X/view?usp=sharing
 */
public class TransactionsListInput extends BaseGraphInput {
	
	/**
	 * Control whether to return a list of rows or a single stat value
	 *
	 */
	public enum RenderMode
	{
		
		/**
		 * Output the volume of transactions matching the target state (e.t. OK, Slowing, Critical)
		 */
		
		SingleStat,
		
		/**
		 * Output a tooltip description of transactions matching the target state (e.t. OK, Slowing, Critical)
		 */
		SingleStatDesc,
		
		
		/**
		 * Output the volume of transactions
		 */
		SingleStatVolume,
		
		/**
		 * Output a weighted avg of transaction response time
		 */
		SingleStatAvg,
		
		/**
		 * Output a weighted avg of transaction baseline response time
		 */
		SingleStatBaselineAvg,
		
		/**
		 * Output a row for each transaction
		 */
		Grid
	}
	
	/**
	 * A link pointing to the most recent Timer event set for the current entry point / trasnaction
	 * that has exceeded its threshold
	 */
	public static final String LINK = "link";
	
	/**
	 * The name expressed as class.method of the current transaction
	 */
	public static final String TRANSACTION = "transaction";
	
	/**
	 * The number of calls into the selected transaction
	 */
	public static final String TOTAL = "invocations";
	
	/**
	 * The avg response time in ms for the completion of the execution of the current transaction
	 * in in the selected time frame
	 */
	public static final String AVG_RESPONSE = "avg_response";
	
	/**
	 * The state of the transaction, either OK, SLOWING, or CRITICAL which describes based
	 * on the transaction slowdown algorithm the performance state of the current transaction in
	 * comparison to its respective baseline.
	 */
	public static final String SLOW_STATE = "slow_state";
	
	/**
	 * The ratio between calls into the current transaction (invocations) and the volume of errors
	 * of the type defined in the Settings dashboard a "transaction failure types".
	 */
	public static final String ERROR_RATE = "error_rate";
	
	/**
	 * the volume of errors of the type defined in the Settings dashboard a "transaction failure types".
	 */
	public static final String ERRORS = "errors";
	
	/**
	 * The diff in rate between the active and baseline window
	 */
	public static final String ERROR_RATE_DELTA = "error_rate_state";
	
	/**
	 * An enum state comparing the rate between the active and baseline window 
	 * with regression delta settings to output: OK, WARN, CRITICAL
	 */
	public static final String ERROR_RATE_DELTA_STATE = "error_rate_delta_state";
	
	/**
	 * The desc of the diff in rate between the active and baseline window
	 */
	public static final String ERROR_RATE_DELTA_DESC = "error_rate_delta_desc";
	
	/**
	 * a text description of the volume of errors of the type defined in the Settings dashboard a "transaction failure types".
	 */
	public static final String ERRORS_DESC = "error_description";
	
	/**
	 * A string describing the change in performance between the current time range and the baseline time frange.
	 */
	public static final String DELTA_DESC = "delta_description";
	
	/**
	 * The avg response time during the baseline time range.
	 */
	public static final String BASELINE_AVG = "baseline_avg";
	
	/**
	 * The number of calls for the current transaction during the baseline time range.
	 */
	public static final String BASELINE_CALLS = "baseline_calls";
	
	/**
	 * The number of calls into the selected transaction in human readable format
	 */
	public static final String ACTIVE_CALLS = "active_calls";
	
	/**
	 * A comma delimited array defining which fields to add for each returned row. If no value
	 * is specified, all fields are added.
	 */
	public String fields;
	
	/**
	 * Control whether to return a list of rows or a single aggregated stat by this function
	 */
	public RenderMode renderMode;
	
	public RenderMode getRenderMore() {
		
		if (renderMode == null) {
			return RenderMode.SingleStat;
		}
		
		return renderMode;	
	}
	
	/**
	 * A String format to be used when formatting the result of a single stat function call. 
	 */
	public String singleStatFormat;
	
	/**
	 * A comma delimited array of performance states, that a target transaction must meet in order to be returned
	 * by this function. Possible values are: 	NO_DATA, OK, SLOWING, CRITICAL
	 */
	public String performanceStates;
	
	/**
	 * The number of graph points to use when calculating event failures. If 0, the 
	 * pointsWanted field will be used.
	 */
	public int eventPointsWanted;
	
	public static final List<String> FIELDS = Arrays.asList(new String[] { 
			LINK, TRANSACTION, TOTAL, AVG_RESPONSE, BASELINE_AVG, BASELINE_CALLS, ACTIVE_CALLS, SLOW_STATE,
			DELTA_DESC, ERROR_RATE, ERRORS, ViewInput.TIME_RANGE });
	
	
	public static Collection<PerformanceState> getStates(String performanceStates) {
		
		List<PerformanceState> result = new ArrayList<PerformanceState>();
		
		if (performanceStates != null) {
			
			String[] parts = performanceStates.split(GrafanaFunction.GRAFANA_SEPERATOR);
			
			for (String part : parts) {
				PerformanceState state = PerformanceState.valueOf(part);
				
				if (state == null) {
					throw new IllegalStateException("Unsupported state " + part + " in " + performanceStates);
				}
				
				result.add(state);
			}
		} else {
			for (PerformanceState state : PerformanceState.values()) {
				result.add(state);
			}
		}
		
		return result;
	}
	
}
