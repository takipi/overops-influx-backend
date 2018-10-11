package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import com.google.gson.Gson;
import com.takipi.common.api.ApiClient;
import com.takipi.integrations.grafana.functions.GrafanaFunction.FunctionFactory;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.output.ResultContent;
import com.takipi.integrations.grafana.output.Series;

public class FunctionParser {
	
	private static final Map<String, FunctionFactory> factories;

	protected static class FunctionAsyncTask implements Callable<FunctionResult> {

		protected ApiClient apiClient; 
		protected String query;
		
		protected FunctionAsyncTask(ApiClient apiClient, String query) {
			this.apiClient = apiClient;
			this.query = query;
		}
		
		@Override
		public FunctionResult call() throws Exception {
			return new FunctionResult(processSingleQuery(apiClient, query));
		}
	}
	
	protected static class FunctionResult {
		protected List<Series> data;
		
		protected FunctionResult(List<Series> data) {
			this.data = data;
		}
	}
	
	public static List<Series> processSingleQuery(ApiClient apiClient, String query) {
		
		String trimmedQuery = query.trim();
		int parenthesisIndex = trimmedQuery.indexOf('(');

		if (parenthesisIndex == -1) {
			throw new IllegalArgumentException("Missing opening parenthesis: " + query);
		}

		char endChar = trimmedQuery.charAt(trimmedQuery.length() - 1);

		if (endChar != ')') {
			throw new IllegalArgumentException("Missing closing parenthesis: " + query);
		}

		String name = trimmedQuery.substring(0, parenthesisIndex);
		String params = trimmedQuery.substring(parenthesisIndex + 1, trimmedQuery.length() - 1);

		FunctionFactory factory = factories.get(name);
		
		if (factory == null) {
			throw new IllegalArgumentException("Unsupported function " + name);
		}
	
		String json = params.replace("\\", "");
		FunctionInput input = (FunctionInput)(new Gson().fromJson(json, factory.getInputClass()));
		GrafanaFunction function = factory.create(apiClient);
		
		return function.process(input);
	}
		
	public static QueryResult processQuery(ApiClient apiClient, String query) {
		if ((query == null) || (query.length() == 0)) {
			throw new IllegalArgumentException("Missing query");
		}

		String trimmedQuery = query.trim();

		String[] singleQueries = trimmedQuery.split(";");
		
		if (singleQueries.length == 1) {
			return createQueryResults(processSingleQuery(apiClient, singleQueries[0]));
		}
				
		CompletionService<FunctionResult> completionService = new ExecutorCompletionService<FunctionResult>(GrafanaFunction.executor);

		for (String singleQuery : singleQueries) {	
			completionService.submit(new FunctionAsyncTask(apiClient, singleQuery));			
		}
		
		int received = 0;
		List<Series> series = new ArrayList<Series>();

		while (received < singleQueries.length) {			
			try {
				Future<FunctionResult> future = completionService.take();
				received++;
				FunctionResult asynResult = future.get();
				series.addAll(asynResult.data);
			} catch (Exception e) {
				throw new IllegalStateException(e);
			} 
		}
		
		return createQueryResults(series);
	}
	
	private static QueryResult createQueryResults(List<Series> series) {
		ResultContent resultContent = new ResultContent();
		resultContent.statement_id = 0;
		resultContent.series = series;
		QueryResult result = new QueryResult();
		result.results = Collections.singletonList(resultContent);

		return result;
	}
	
	public static void registerFunction(FunctionFactory factory) {
		factories.put(factory.getName(), factory);
	}
	
	static {
		factories = new HashMap<String, FunctionFactory>();
		
		//widget functions
		registerFunction(new EventsFunction.Factory());
		registerFunction(new GraphFunction.Factory());
		registerFunction(new GroupByFunction.Factory());
		registerFunction(new VolumeFunction.Factory());
		registerFunction(new CategoryFunction.Factory());
		
		//variable functions
		registerFunction(new EnvironmentsFunction.Factory());
		registerFunction(new ApplicationsFunction.Factory());
		registerFunction(new ServersFunction.Factory());
		registerFunction(new DeploymentsFunction.Factory());
		registerFunction(new ViewsFunction.Factory());
	}
	
}
