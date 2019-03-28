package com.takipi.integrations.grafana.input;

import java.util.Arrays;
import java.util.List;

public class QueryDiagnosticsInput extends FunctionInput {
	
	public enum ReportMode {
		ApiCache,
		RegressionCache,
		Log,
		Threads
	}
	
	public enum OutputMode {
		SingleStat,
		Grid,
	}
	
	public OutputMode outputMode;
	
	public OutputMode getOutputMode() {
		
		if (outputMode == null) {
			return OutputMode.Grid;
		}
		
		return outputMode;
	}
	
	public ReportMode reportMode;
	
	public ReportMode getReportMode() {
		
		if (reportMode == null) {
			return ReportMode.ApiCache;
		}
		
		return reportMode;
	}
		
	public static final List<String> CACHE_FIELDS = Arrays.asList(
		new String[] { 	
			"Time",
			"Duration",
			"ApiHash",
			"Key",
			"Value"
		});
	
	public static final List<String> LOG_FIELDS = Arrays.asList(
			new String[] { 	
				"Time",
				"Duration",
				"ApiHash",
				"Message"
			});
	
	public static final List<String> THREAD_FIELDS = Arrays.asList(
			new String[] { 	
				
				"ApiHash",
				
				"FunctionActiveThreadSize",
				"FunctionThreadPoolSize",
				
				"QueryActiveThreadSize",
				"QueryThreadPoolSize",
				
				"FunctionQueueSize",
				"QueryQueueSize"
			});
}
