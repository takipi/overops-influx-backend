package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint;
import com.takipi.api.client.request.transaction.TransactionsGraphRequest;
import com.takipi.api.client.result.transaction.TransactionsGraphResult;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.GraphFunction.SeriesVolume;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsGraphInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings.GroupFilter;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TransactionsGraphFunction extends BaseGraphFunction {

	public enum GraphType {
		avg_time, invocations, all
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionsGraphInput.class;
		}

		@Override
		public String getName() {
			return "transactionsGraph";
		}
	}
	
	protected static class TimeAvg {
		protected long invocations;
		protected double avgTime;
	}


	public TransactionsGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

    @Override
	protected List<GraphSeries> processServiceGraph(String serviceId, String viewId, String viewName,
			BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds, int pointsWanted) {

		TransactionsGraphInput input = (TransactionsGraphInput) request;

		Pair<String, String> fromTo = TimeUtil.toTimespan(timeSpan);
				
		TransactionsGraphRequest.Builder builder = TransactionsGraphRequest.newBuilder().setServiceId(serviceId)
				.setViewId(viewId).setFrom(fromTo.getFirst()).setTo(fromTo.getSecond())
				.setWantedPointCount(pointsWanted);
				
		applyFilters(request, serviceId, builder);

		Response<TransactionsGraphResult> response = ApiCache.getTransactionsGraph(apiClient, serviceId, input, pointsWanted, builder.build());
				
		validateResponse(response);
		
		if ((response.data == null) || (response.data.graphs == null)) { 

			return Collections.emptyList();
		}

		Collection<String> transactions = getTransactions(serviceId, input, timeSpan);
		
		List<GraphSeries> result = processServiceGraph(serviceId, input, 
				serviceIds, transactions, response.data.graphs);
		

		return result;
	}
    
	protected List<GraphSeries> processServiceGraph(String serviceId, 
			TransactionsGraphInput input, Collection<String> serviceIds, Collection<String> transactions,
			Collection<TransactionGraph> graphs) {
    	
		List<GraphSeries> result;

		GroupFilter transactionsFilter = GrafanaSettings.getServiceSettings(apiClient, serviceId).getTransactionsFilter(transactions);
		
		if ((input.aggregate) || (CollectionUtil.safeIsEmpty(transactions))) { 
			
			String seriesName = getSeriesName(input, input.seriesName, input.volumeType, serviceId, serviceIds);
			result = Collections.singletonList(createAggregateGraphSeries(serviceId, graphs, transactionsFilter,
					input, serviceIds, seriesName));
		} else {
			result = createMultiGraphSeries(serviceId, graphs, input.volumeType, serviceIds,
					transactionsFilter);
		}
		
		return result;
    }
    
    /**
	 * @param timeSpan - needed for children  
	 */
    protected Collection<String> getTransactions(String serviceId, TransactionsGraphInput input,
    		Pair<DateTime, DateTime> timeSpan) {
    		
    		Collection<String> result;

		if (input.hasTransactions()) {
			result = input.getTransactions(serviceId);
		} else {
			result = null;
		}
		
		return result;
    }

	protected List<GraphSeries> createMultiGraphSeries(String serviceId, Collection<TransactionGraph> graphs,
			GraphType volumeType, Collection<String> serviceIds, GroupFilter transactionFilter) {

		List<GraphSeries> result = new ArrayList<GraphSeries>();

		for (TransactionGraph graph : graphs) {

			Pair<String, String> nameAndMethod = getFullNameAndMethod(graph.name);
			
			if (filterTransaction(transactionFilter, nameAndMethod.getFirst(), nameAndMethod.getSecond()))
			{
				continue;
			}

			if (volumeType.equals(GraphType.all)) {
				result.add(createTransactionGraphSeries(serviceId, graph, GraphType.avg_time, serviceIds));
				result.add(createTransactionGraphSeries(serviceId, graph, GraphType.invocations, serviceIds));
			} else {
				result.add(createTransactionGraphSeries(serviceId, graph, volumeType, serviceIds));

			}
		}

		return result;
	}
	
	private SeriesVolume getAvgSeriesValues(Collection<TransactionGraph> graphs,
			GroupFilter transactionsFilter) {
		
		Collection<TransactionGraph>  targetGraphs;
		
		if (transactionsFilter != null) {
			
			targetGraphs = new ArrayList<TransactionGraph>(graphs.size());
			
			for (TransactionGraph graph : graphs) {
		
				Pair<String, String> nameAndMethod = getFullNameAndMethod(graph.name);
				
				if (filterTransaction(transactionsFilter, nameAndMethod.getFirst(), nameAndMethod.getSecond()))
				{
					continue;
				}
				
				targetGraphs.add(graph);
			}
		} else {
			targetGraphs = graphs;
		}
			
		Map<String, TimeAvg> timeAvgMap = new HashMap<String, TimeAvg>();

		for (TransactionGraph graph : targetGraphs) {
			
			for (GraphPoint gp : graph.points) {
				TimeAvg timeAvg = timeAvgMap.get(gp.time);
				
				if (timeAvg == null) {
					timeAvg = new TimeAvg();
					timeAvgMap.put(gp.time, timeAvg);
				} 
				
				timeAvg.invocations += gp.stats.invocations;
			}
		}

		for (TransactionGraph graph : targetGraphs) {
			
			for (GraphPoint gp : graph.points) {
				TimeAvg timeAvg = timeAvgMap.get(gp.time);
				
				if (timeAvg.invocations == 0) {
					continue;
				}
				
				timeAvg.avgTime += gp.stats.avg_time * gp.stats.invocations / timeAvg.invocations;
				
				if (Double.isNaN(timeAvg.avgTime)) {
					throw new IllegalStateException();
				}
			}
		}

		List<List<Object>> result = new ArrayList<List<Object>>(timeAvgMap.size());
		Map<Long, TimeAvg> sortedAvgMap = new TreeMap<Long, TimeAvg>();

		for (Map.Entry<String, TimeAvg> entry : timeAvgMap.entrySet()) {
			
			String time = entry.getKey();
			TimeAvg timeAvg = entry.getValue();
			
			DateTime gpTime = ISODateTimeFormat.dateTimeParser().parseDateTime(time);
			Long epochTime = Long.valueOf(gpTime.getMillis());
			
			sortedAvgMap.put(epochTime, timeAvg);
		}
				
		for (Map.Entry<Long, TimeAvg> entry : sortedAvgMap.entrySet()) {
			
			Long time = entry.getKey();
			TimeAvg timeAvg = entry.getValue();
	
			result.add(Arrays.asList(new Object[] { time, Double.valueOf(timeAvg.avgTime) }));
		}

		return SeriesVolume.of(result, 0L);
	}

	private SeriesVolume getInvSeriesValues(Collection<TransactionGraph> graphs,
			GroupFilter transactionFilter) {

		Map<Long, Long> values = new TreeMap<Long, Long>();
		
		for (TransactionGraph graph : graphs) {
			
			Pair<String, String> nameAndMethod = getFullNameAndMethod(graph.name);
			
			if (filterTransaction(transactionFilter, nameAndMethod.getFirst(), nameAndMethod.getSecond()))
			{
				continue;
			}
			
			for (GraphPoint gp : graph.points) {
				DateTime gpTime = ISODateTimeFormat.dateTimeParser().parseDateTime(gp.time);
				Long epochTime = Long.valueOf(gpTime.getMillis());

				Long value = values.get(epochTime);

				if (value == null) {
					value = Long.valueOf(gp.stats.invocations);

				} else {
					value = Long.valueOf(value.longValue() + gp.stats.invocations);
				}

				values.put(epochTime, value);
			}
		}

		long volume = 0;
		List<List<Object>> result = new ArrayList<List<Object>>();

		for (Map.Entry<Long, Long> entry : values.entrySet()) {
			
			Long time = entry.getKey();
			Long value = entry.getValue();

			result.add(Arrays.asList(new Object[] { time, value }));
			volume += value.longValue();
		}

		return SeriesVolume.of(result, Long.valueOf(volume));
	}

	/**
	 * @param serviceId  
	 * @param serviceIds - needed for children
	 */
	protected GraphSeries createAggregateGraphSeries(String serviceId, Collection<TransactionGraph> graphs,
			GroupFilter transactionFilter, TransactionsGraphInput input, 
			Collection<String> serviceIds, String seriesName) {

		Series series = new Series();
			
		SeriesVolume seriesValues;

		if ((input.volumeType == null) || (input.volumeType.equals(GraphType.avg_time))) {
			seriesValues = getAvgSeriesValues(graphs, transactionFilter);

		} else {
			seriesValues = getInvSeriesValues(graphs, transactionFilter);
		}

		series.name = EMPTY_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, seriesName });
		series.values = seriesValues.values;

		return GraphSeries.of(series, seriesValues.volume);

	}

	private GraphSeries createTransactionGraphSeries(String serviceId, TransactionGraph graph, GraphType volumeType,
			Collection<String> serviceIds) {

		Series series = new Series();
		
		String tagName = getServiceValue(getTransactionName(graph.name, true), serviceId, serviceIds);
		
		series.name = EMPTY_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, tagName });

		SeriesVolume seriesData = processGraphPoints(graph, volumeType);

		series.values = seriesData.values;

		return GraphSeries.of(series, seriesData.volume);
	}
	
	private SeriesVolume processGraphPoints(TransactionGraph graph, GraphType volumeType) {

		long volume = 0;
		List<List<Object>> values = new ArrayList<List<Object>>(graph.points.size());

		for (GraphPoint gp : graph.points) {
			
			DateTime gpTime = ISODateTimeFormat.dateTimeParser().parseDateTime(gp.time);

			double value;

			if (volumeType.equals(GraphType.avg_time)) {
				value = gp.stats.avg_time;
			} else {
				value = gp.stats.invocations;
			}

			volume += value;
			values.add(Arrays.asList(new Object[] { Long.valueOf(gpTime.getMillis()), value }));
		}

		return SeriesVolume.of(values, Long.valueOf(volume));
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof TransactionsGraphInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		return super.process(functionInput);
	}
}
