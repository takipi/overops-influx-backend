package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import com.takipi.api.client.ApiClient;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.QueryDiagnosticsInput;
import com.takipi.integrations.grafana.input.QueryDiagnosticsInput.OutputMode;
import com.takipi.integrations.grafana.input.QueryDiagnosticsInput.ReportMode;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.ApiCache.BaseCacheLoader;
import com.takipi.integrations.grafana.util.ApiCache.QueryLogItem;
import com.takipi.integrations.grafana.util.ApiCache.RegressionCacheLoader;
import com.takipi.integrations.grafana.util.TimeUtil;

public class QueryDiagnosticsFunction extends GrafanaFunction {
	public QueryDiagnosticsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new QueryDiagnosticsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return QueryDiagnosticsInput.class;
		}

		@Override
		public String getName() {
			return "queryDiagnostics";
		}
	}

	private List<Series> processSingleStat(QueryDiagnosticsInput input) {
		
		ReportMode reportMode = input.getReportMode();
		
		Object value;
		
		switch (reportMode) {
			
			case ApiCache:
				value = ApiCache.queryCache.size();
				break;
			case RegressionCache:
				value = ApiCache.regressionOutputCache.size();
				break;
			case Log:
				value = ApiCache.queryLogItems.size();
				break;
			case Threads:
				value = GrafanaThreadPool.executorCache.size();
				break;
			default:
				throw new IllegalStateException(String.valueOf(reportMode));
			
		}
		
		return createSingleStatSeries(Pair.of(TimeUtil.now(), TimeUtil.now()), value);
		
	}
	
	private List<String> getColumns(ReportMode reportMode) {
		
		switch (reportMode) {
			
			case RegressionCache:
			case ApiCache:
				return QueryDiagnosticsInput.CACHE_FIELDS;
			case Log:
				return QueryDiagnosticsInput.LOG_FIELDS;
			case Threads:
				return QueryDiagnosticsInput.THREAD_FIELDS;
			default:
				throw new IllegalStateException(String.valueOf(reportMode));	
		}
	}
	
	private 	List<List<Object>> getLogValues() {
		
		List<QueryLogItem> items = new ArrayList<QueryLogItem>(ApiCache.queryLogItems);
		List<List<Object>> result = new ArrayList<List<Object>>(items.size());
		
		for (QueryLogItem queryLogItem : items) {
						
			result.add(Arrays.asList(new Object[] {
				queryLogItem.t1,
				Long.valueOf(Math.max(0, queryLogItem.t2 - queryLogItem.t1)),
				queryLogItem.apiHash,
				queryLogItem.toString()
			}));
		}
		
		return result;
	}
	
	private 	List<List<Object>> getThreadValues() {
				
		List<Map.Entry<ApiClient, Pair<ThreadPoolExecutor, ThreadPoolExecutor>>> items = 
			new ArrayList<Map.Entry<ApiClient, Pair<ThreadPoolExecutor,
			ThreadPoolExecutor>>>(GrafanaThreadPool.executorCache.asMap().entrySet());
		
		List<List<Object>> result = new ArrayList<List<Object>>(items.size());

		for (Map.Entry<ApiClient, Pair<ThreadPoolExecutor, ThreadPoolExecutor>> entry : items) {
			
			ThreadPoolExecutor functionPool = entry.getValue().getFirst();
			ThreadPoolExecutor queryPool = entry.getValue().getSecond();

			
			result.add(Arrays.asList(new Object[] {
					entry.getKey().hashCode(),
					
					functionPool.getActiveCount(),
					functionPool.getPoolSize(),
					
					queryPool.getActiveCount(),
					queryPool.getPoolSize(),
					
					functionPool.getQueue().size(),
					queryPool.getQueue().size()

				}));

		}
		
		return result;
	}
	
	private 	List<List<Object>> getQueryCacheValues() {
		
		List<Map.Entry<BaseCacheLoader, Response<?>>> items = 
			new ArrayList<Map.Entry<BaseCacheLoader, Response<?>>>(ApiCache.queryCache.asMap().entrySet());
		
		List<List<Object>> result = new ArrayList<List<Object>>(items.size());

		for (Map.Entry<BaseCacheLoader, Response<?>> entry : items) {
			
			result.add(Arrays.asList(new Object[] {
					entry.getKey().loadT1,
					Long.valueOf(entry.getKey().loadT2 - entry.getKey().loadT1),
					entry.getKey().apiClient.hashCode(),
					entry.getKey().toString(),
					entry.getKey().getLoaderData(entry.getValue())

				}));

		}
		
		return result;
	}
	
	private 	List<List<Object>> getRegressionCacheValues() {
		
		List<Map.Entry<RegressionCacheLoader, RegressionOutput>> items =
			new ArrayList<Map.Entry<RegressionCacheLoader, 
			RegressionOutput>>(ApiCache.regressionOutputCache.asMap().entrySet());
		
		List<List<Object>> result = new ArrayList<List<Object>>(items.size());
		
		for (Map.Entry<RegressionCacheLoader, RegressionOutput> entry : items) {
			
			result.add(Arrays.asList(new Object[] {
					entry.getKey().loadT1,
					Long.valueOf(Math.max(0, entry.getKey().loadT2 - entry.getKey().loadT1)),
					entry.getKey().apiClient.hashCode(),
					entry.getKey().toString(),
					entry.getKey().getLoaderData(entry.getValue())

				}));

		}
		
		return result;
	}

	
	private List<Series> processGrid(QueryDiagnosticsInput input) {
		
		ReportMode reportMode = input.getReportMode();
		Series series = createSeries(new ArrayList<List<Object>>(), getColumns(reportMode));
				
		switch (reportMode) {
			
			case ApiCache:
				series.values = getQueryCacheValues();
				break;
			case RegressionCache:
				series.values = getRegressionCacheValues();
				break;
			case Log:
				series.values = getLogValues();
				break;
			case Threads:
				series.values = getThreadValues();
				break;
			default:
				throw new IllegalStateException(String.valueOf(reportMode));
			
		}

		return Collections.singletonList(series);
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof QueryDiagnosticsInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		QueryDiagnosticsInput input = (QueryDiagnosticsInput)functionInput;
		
		OutputMode outputMode = input.getOutputMode();
		
		switch (outputMode) {
			
			case Grid:
				return processGrid(input);
			case SingleStat:
				return processSingleStat(input);
			default:
				throw new IllegalStateException(String.valueOf(outputMode));
			
		}
	}
}
