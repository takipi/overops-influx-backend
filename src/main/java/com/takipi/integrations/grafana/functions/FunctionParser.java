package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.functions.GrafanaFunction.FunctionFactory;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.output.ResultContent;
import com.takipi.integrations.grafana.output.Series;

public class FunctionParser {
	
	private static final String QUERY_SEPERATOR = ";";
	
	private static final Map<String, FunctionFactory> factories;

	protected static class FunctionAsyncTask extends BaseAsyncTask implements Callable<FunctionResult> {

		protected ApiClient apiClient; 
		protected String query;
		protected int index;
		
		protected FunctionAsyncTask(ApiClient apiClient, String query, int index) {
			this.apiClient = apiClient;
			this.query = query;
			this.index = index;
		}
		
		@Override
		public FunctionResult call() throws Exception {
			
			beforeCall();
			
			try {
				return new FunctionResult(processSingleQuery(apiClient, query), index);
			} finally {
				afterCall();
			}
		}
		
		@Override
		public String toString() {
			return query;
		}
	}
	
	protected static class FunctionResult {
		protected List<Series> data;
		protected int index;
		
		protected FunctionResult(List<Series> data, int index) {
			this.data = data;
			this.index = index;
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

		String[] singleQueries = trimmedQuery.split(QUERY_SEPERATOR);
		
		QueryResult result = new QueryResult();

		if (singleQueries.length == 1) {
			ResultContent resultContent = new ResultContent();
			resultContent.series = processSingleQuery(apiClient, singleQueries[0]);
			result.results = Collections.singletonList(resultContent);
			return result;
		}
				
		CompletionService<FunctionResult> completionService = new ExecutorCompletionService<FunctionResult>(GrafanaThreadPool.executor);

		int index = 0;
		
		for (String singleQuery : singleQueries) {	
			completionService.submit(new FunctionAsyncTask(apiClient, singleQuery, index));	
			index++;
		}
		
		int received = 0;
		result.results = new ArrayList<ResultContent>();
			
		while (received < singleQueries.length) {			
			try {
				Future<FunctionResult> future = completionService.take();
				FunctionResult asynResult = future.get();
				
				ResultContent resultContent = new ResultContent();
				resultContent.series = asynResult.data;
				resultContent.statement_id = asynResult.index;
				result.results.add(resultContent);
				
				received++;

			} catch (Exception e) {
				throw new IllegalStateException(e);
			} 
		}
		
		result.results.sort(new Comparator<ResultContent>() {

			@Override 
			public int compare(ResultContent o1, ResultContent o2) {
				return o1.statement_id - o2.statement_id;
			}
		});
		
		return result;
	}
	
	public static void registerFunction(FunctionFactory factory) {
		factories.put(factory.getName(), factory);
	}
	
	static {
		factories = new HashMap<String, FunctionFactory>();
		
		//event functions
		registerFunction(new EventsFunction.Factory());
		registerFunction(new GraphFunction.Factory());
		registerFunction(new GroupByFunction.Factory());
		registerFunction(new VolumeFunction.Factory());
		registerFunction(new CategoryFunction.Factory());
		
		//transaction functions
		registerFunction(new TransactionsVolumeFunction.Factory());
		registerFunction(new TransactionsGraphFunction.Factory());
		registerFunction(new TransactionsListFunction.Factory());
		registerFunction(new TransactionsRateFunction.Factory());

		//variable functions
		registerFunction(new EnvironmentsFunction.Factory());
		registerFunction(new ApplicationsFunction.Factory());
		registerFunction(new ServersFunction.Factory());
		registerFunction(new DeploymentsFunction.Factory());
		registerFunction(new ViewsFunction.Factory());
		registerFunction(new TransactionsFunction.Factory());
		registerFunction(new LabelsFunction.Factory());
	}
	
}
