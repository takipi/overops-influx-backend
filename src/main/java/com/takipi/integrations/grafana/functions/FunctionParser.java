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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.functions.GrafanaFunction.FunctionFactory;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.output.ResultContent;
import com.takipi.integrations.grafana.output.Series;

public class FunctionParser {
	private static final Logger logger = LoggerFactory.getLogger(FunctionParser.class);
	
	private static final String QUERY_SEPERATOR = ";";
	
	private static final Map<String, FunctionFactory> factories;

	protected static class FunctionAsyncTask extends BaseAsyncTask implements Callable<Object>  {

		protected ApiClient apiClient; 
		protected String query;
		protected int index;
		
		protected FunctionAsyncTask(ApiClient apiClient, String query, int index) {
			this.apiClient = apiClient;
			this.query = query;
			this.index = index;
		}
		
		@Override
		public Object call() throws Exception {
			
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

		int endIndex = trimmedQuery.lastIndexOf(')');

		if (endIndex == -1) {
			throw new IllegalArgumentException("Missing closing parenthesis: " + query);
		}

		String name = trimmedQuery.substring(0, parenthesisIndex);
		String params = trimmedQuery.substring(parenthesisIndex + 1, endIndex);

		FunctionFactory factory = factories.get(name);
		
		if (factory == null) {
			throw new IllegalArgumentException("Unsupported function " + name);
		}
	
		String json = params.replace("\\", "");
		
		FunctionInput input;
		GrafanaFunction function;
		
		try {
			input = (FunctionInput)(new Gson().fromJson(json, factory.getInputClass()));
			function = factory.create(apiClient);
		} catch (Exception e) {
			throw new IllegalStateException("Could not parse query: " + e.toString() + " query:" + json, e);
		}
		
		logger.debug("OO-AS-INFLUX | About to process {} with input {}", function, input);
		
		List<Series> result;
		
		try {
			result = function.process(input);
		} catch (Exception e) {
			throw new IllegalStateException("Could not process query: " + e.toString() + " query:" + json, e);
		}
		
		return result;
	}
		
	private static List<String> getQueries(String query) {
		String trimmedQuery = query.trim();
		String[] splitQueries = trimmedQuery.split(QUERY_SEPERATOR);
		
		List<String> result = new ArrayList<String>(splitQueries.length);
		
		for (String splitQuery : splitQueries) {
			if (!splitQuery.trim().isEmpty()) {
				result.add(splitQuery);
			}
		}
		
		return result;
	}
	
	public static QueryResult processSync(ApiClient apiClient, String query) {
		
		QueryResult result = new QueryResult();

		ResultContent resultContent = new ResultContent();
		resultContent.series = processSingleQuery(apiClient, query);
		result.results = Collections.singletonList(resultContent);
		
		return result;	
	}
	
	public static QueryResult processAsync(ApiClient apiClient, List<String> singleQueries ) {
		
		QueryResult result = new QueryResult();

		CompletionService<Object> completionService = new ExecutorCompletionService<Object>(GrafanaThreadPool.getFunctionExecutor(apiClient));

		int index = 0;
		
		for (String singleQuery : singleQueries) {	
			completionService.submit(new FunctionAsyncTask(apiClient, singleQuery, index++));	
		}
		
		int received = 0;
		result.results = new ArrayList<ResultContent>();
			
		while (received < singleQueries.size()) {			
			try {
				Future<Object> future = completionService.take();
				FunctionResult asynResult = (FunctionResult)(future.get());
				
				ResultContent resultContent = new ResultContent();
				resultContent.series = asynResult.data;
				resultContent.statement_id = asynResult.index;
				result.results.add(resultContent);
				
				received++;

			} catch (Exception e) {
				throw new IllegalStateException(e);
			} 
		}
		
		sortStatements(result.results);
		
		return result;
	}
	
	public static QueryResult processQuery(ApiClient apiClient, String query) {
		
		if ((query == null) || (query.length() == 0)) {
			throw new IllegalArgumentException("Missing query");
		}

		List<String> singleQueries = getQueries(query);
		
		if (singleQueries.size() == 1) {
			return processSync(apiClient, singleQueries.get(0));
		} else {
			return processAsync(apiClient, singleQueries);
		}		
	}
	
	private static void sortStatements(List<ResultContent> results) {
		results.sort(new Comparator<ResultContent>() {

			@Override 
			public int compare(ResultContent o1, ResultContent o2) {
				return o1.statement_id - o2.statement_id;
			}
		});
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
		registerFunction(new EventsDiffFunction.Factory());
		// registerFunction(new EventsDiffDescFunction.Factory());
		
		//Routing graphs
		registerFunction(new CategoryFunction.Factory());
		registerFunction(new TypesGraph.Factory());
		registerFunction(new RoutingGraphFunction.Factory());
		registerFunction(new SplitGraphFunction.Factory());
		
		//regression functions
		registerFunction(new RegressionFunction.Factory());
		registerFunction(new ReliabilityReportFunction.Factory());
		registerFunction(new RegressionGraphFunction.Factory());
		registerFunction(new BaselineWindowFunction.Factory());
		registerFunction(new BaselineAnnotationFunction.Factory());
		registerFunction(new RegressedEventsFunction.Factory());


		//deployment functions
		registerFunction(new DeploymentsGraphFunction.Factory());
		registerFunction(new DeploymentNameFunction.Factory());
		registerFunction(new DeploymentsAnnotation.Factory());
				
		//transaction functions
		registerFunction(new TransactionsVolumeFunction.Factory());
		registerFunction(new TransactionsGraphFunction.Factory());
		registerFunction(new TransactionsListFunction.Factory());
		registerFunction(new TransactionsRateFunction.Factory());
		registerFunction(new KeyTransactionsGraphFunction.Factory());
		registerFunction(new TransactionAvgGraphFunction.Factory());
		registerFunction(new SlowTransactionsFunction.Factory());
		registerFunction(new TransactionsDiffFunction.Factory());
	
		//variable filter functions
		registerFunction(new EnvironmentsFunction.Factory());
		registerFunction(new ApplicationsFunction.Factory());
		registerFunction(new ServersFunction.Factory());
		registerFunction(new DeploymentsFunction.Factory());
		registerFunction(new EventTypesFunction.Factory());
		registerFunction(new TransactionsFunction.Factory());
		
		//variable metadata functions
		registerFunction(new ApiHostFunction.Factory());
		registerFunction(new ViewsFunction.Factory());
		registerFunction(new LabelsFunction.Factory());
		
		//variable settings functions
		registerFunction(new EnvironmentSettingsFunction.Factory());
		registerFunction(new VariableRedirectFunction.Factory());
		registerFunction(new SettingsVarFunction.Factory());
		registerFunction(new LimitVariableFunction.Factory());
		
		//cost functions
		registerFunction(new CostCalculatorFunction.Factory());
		registerFunction(new CostSplitGraphFunction.Factory());
		registerFunction(new GraphCostPieChartFunction.Factory());
	}
}
