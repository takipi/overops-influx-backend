package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.takipi.common.util.CollectionUtil;
import com.takipi.integrations.grafana.functions.GrafanaFunction;

/** 
 * A function returning a table, graph or single stat relating to the number of new and increasing issues
 * within a target set of events. The determination of which events have regressed is governed by the regression
 * configuration in the Settings dashboard.
 * 
 * Example query to return table:
 * 
 * 		regressions({"fields":"link,type,entry_point,introduced_by,severity,regression,
 * 		typeMessage,error_location,stats.hits,rate,regDelta,first_seen,id", 
 * 		"render":"Grid", "regressionTypes":"Regressions,SevereRegressions", 
 * 		"timeFilter":"$timeFilter","environments":"$environments",
 * 		"applications":"$applications","servers":"$servers","deployments":"$deployments",
 * 		"view":"$view","maxColumnLength":90,"pointsWanted":"$pointsWanted","
 * 		types":"$type","searchText":"$search"})
 * 
 * Example query for single stat:
 * 
 * 		regressionReport({"timeFilter":"$timeFilter", "environments":"$environments",
 * 		"applications":"$applications","servers":"$servers","deployments":"$deployments",
 * 		"view":"$view","transactions":"$transactions","pointsWanted":"$pointsWanted",
 * 		"transactionPointsWanted":"$transactionPointsWanted","types":"$type",
 * 		"render":"SingleStat"})
 * 
 * 	Screenshot: 
 * 		https://drive.google.com/file/d/1n6CVDqhwr0cz6C1p0AbMtAkontVJ1M0P/view?usp=sharing
 * 		https://drive.google.com/file/d/1PZHd27rVwt60pqcRIybgdYJ5qPSQQWB3/view?usp=sharing
 * 		https://drive.google.com/file/d/1Nb1FEXTxoyXAjlwxXNfq8GkjsJYYYHTF/view?usp=sharing
 * 	
 */

public class RegressionsInput extends EventsInput
{
	/**
	 * The types of regression that can be reported by this function
	 */
	public static enum RegressionType
	{
		/**
		 * A severe new issue detected within the context of this regression report
		 */
		SevereNewIssues,
		
		/**
		 * A new issue detected within the context of this regression report
		 */
		NewIssues,
		
		/**
		 * A severe regression detected within the context of this regression report
		 */
		SevereRegressions,
		
		/**
		 * A regression detected within the context of this regression report
		 */
		Regressions	
	}
	
	public enum RenderMode
	{
		/**
		 * return a single stat value
		 */
		SingleStat,
		
		/**
		 * return a desc of the single stat value
		 */
		SingleStatDesc,
		
		/**
		 * return a the count of events returned
		 */
		SingleStatCount,
		
		/**
		 * return the volume of events returned
		 */
		SingleStatVolume,
		
		/**
		 * return a string value of the volume of events returned
		 */
		SingleStatVolumeText,
		
		/**
		 * return a table with each row depicting a new or increasing error
		 */
		Grid,
		
		/**
		 * This mode is only supported if the function is invoked to produce a reliabilty report,
		 * where each point depicts a key value from the report
		 */
		Graph;
	}
	
	/**
	 * Control the type of output to return from the function
	 */
	public RenderMode render;
	
	/**
	 * A comma delimited array of regression types to be included in the result of this analysis.
	 * The first value will be selected is returning a single stat value
	 */
	public String regressionTypes;
	
	/**
	 * An optional string format to be used when returning a single stat value. 
	 * A decimal %d value will be passed into the string format
	 */
	public String singleStatFormat;
	
	/**
	 * Additional fields supported by this functions
	 */
	
	/**
	 * The max number of event tooltips to show on hover
	 */
	public static int MAX_TOOLTIP_ITEMS = 3;
	
	/**
	 * Value of the regression rate
	 */
	public static String REG_DELTA = "reg_delta";
	
	/**
	 * Text description of the regression rate
	 */
	public static String REGRESSION = "regression";
	
	/**
	 * Text value for the severity of the regression.
	 * If the issue is severe = 2
	 * If the issue is non-severe = 1;
	 * If the issue is not new or regressed = 0;
	 */
	public static String SEVERITY = "severity";
	
	/**
	 * Text description of the regression
	 */
	public static String REG_DESC = "reg_desc";
	
	public boolean newOnly() {
		Collection<RegressionType> regressionTypes = getRegressionTypes();
		return newOnly(regressionTypes);
	}

	public static boolean newOnly(Collection<RegressionType> regressionTypes) {
		
		
		if (CollectionUtil.safeIsEmpty(regressionTypes)) {
			return false;
		}
		
		boolean result = (!regressionTypes.contains(RegressionType.Regressions))
				&& (!regressionTypes.contains(RegressionType.SevereRegressions));
		
		return result;
	}
	
	public Collection<RegressionType> getRegressionTypes() {
		
		if (regressionTypes == null)
		{
			return Collections.emptyList();
		}
		
		String[] parts = regressionTypes.split(GrafanaFunction.ARRAY_SEPERATOR);
		Collection<RegressionType> result = new ArrayList<RegressionType>(parts.length);
		
		for (String part : parts)
		{
			RegressionType type = RegressionType.valueOf(part);
			
			if (type != null)
			{
				result.add(type);
			}
		}
		
		return result;
		
	}
}
