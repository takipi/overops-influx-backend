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

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint;
import com.takipi.api.client.util.performance.calc.PerformanceState;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.GraphFunction.SeriesVolume;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsGraphInput;
import com.takipi.integrations.grafana.input.TransactionsGraphInput.AggregateMode;
import com.takipi.integrations.grafana.input.TransactionsGraphInput.GraphType;
import com.takipi.integrations.grafana.input.TransactionsGraphInput.TimeWindow;
import com.takipi.integrations.grafana.input.TransactionsListInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GroupSettings;
import com.takipi.integrations.grafana.settings.GroupSettings.GroupFilter;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TransactionsGraphFunction extends BaseGraphFunction {

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
	
	protected class TransactionGraphsResult {
		protected Collection<TransactionGraph> graphs;
		protected RegressionInput regressionInput;
		protected RegressionWindow regressionWindow;
	}
	
	protected static class TimeAvg {
		protected long invocations;
		protected double avgTime;
	}


	public TransactionsGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	private TransactionGraph mergeGraphs(TransactionData transactionData) {
		
		TransactionGraph result = new TransactionGraph();
		
		result.name = transactionData.graph.name;
		result.class_name = transactionData.graph.class_name;
		result.method_name = transactionData.graph.method_name;
		result.method_desc = transactionData.graph.method_desc;
	
		int size = transactionData.graph.points.size();
		
		if (transactionData.baselineGraph!= null) {
			size += transactionData.baselineGraph.points.size();
		}
		
		result.points = new ArrayList<GraphPoint>(size);
	
		if (transactionData.baselineGraph!= null) {
			result.points.addAll(transactionData.baselineGraph.points);
		}
		
		result.points.addAll(transactionData.graph.points);
		
		return result;
	}

	protected TransactionGraphsResult getTransactionsGraphs(String serviceId, 
		String viewId, Pair<DateTime, DateTime> timeSpan, 
		TransactionsGraphInput input, int pointsWanted) {
		
		
		Collection<TransactionGraph> activeGraphs = getTransactionGraphs(input, serviceId, 
				viewId, timeSpan, input.getSearchText(), pointsWanted, 0, 0);
		
		TimeWindow timeWindow = input.getTimeWindow();
		
		TransactionGraphsResult result = new TransactionGraphsResult();
		
		if ((input.performanceStates != null) || (!TimeWindow.Active.equals(timeWindow))) {	
			
			Collection<PerformanceState> performanceStates = TransactionsListInput.getStates(input.performanceStates);
			
			TransactionDataResult transactionDataResult = getTransactionDatas(activeGraphs, 
				serviceId, viewId, timeSpan, input, false, 0);
			
			if (transactionDataResult == null) {
				return null;
			}
			
			result.graphs = new ArrayList<TransactionGraph>();
			result.regressionWindow = transactionDataResult.regressionWindow;
			result.regressionInput = transactionDataResult.regressionInput; 	
			
			for (TransactionData transactionData : transactionDataResult.items.values()) {
				
				if (!performanceStates.contains(transactionData.state)) {
					continue;
				}
				
				TransactionGraph graph;
				
				switch (timeWindow) {
					case Active:
						graph = transactionData.graph;
						break;

					case Baseline:
						graph = transactionData.baselineGraph;
						break;

					case All:
						graph = mergeGraphs(transactionData);
						break;
						
					default:
						throw new IllegalStateException(timeWindow.toString());
					
				}
						
				if (graph != null) {
					result.graphs.add(graph);
				}
			}			
		} else {
			result.graphs = activeGraphs;
		}
				
		return result;
	}
	
	private TransactionsGraphInput getInput(TransactionsGraphInput input) {
		
		TransactionsGraphInput result;

		if (input.timeFilterVar != null) {
			Gson gson = new Gson();
			result = gson.fromJson(gson.toJson(input), input.getClass());
			Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(input.timeFilterVar);
			result.timeFilter = TimeUtil.getTimeFilter(timespan);
		} else {
			result = input;
		}
		
		return result;
	}
	
    @Override
	protected List<GraphSeries> processServiceGraph( Collection<String> serviceIds, 
			String serviceId, String viewId, String viewName,
			BaseGraphInput input, Pair<DateTime, DateTime> timeSpan, int pointsWanted) {

		TransactionsGraphInput tgInput = (TransactionsGraphInput)(input);
		
		TransactionGraphsResult transactionGraphs = getTransactionsGraphs(serviceId, viewId, timeSpan, tgInput, pointsWanted);
		
		if (transactionGraphs == null) {
			return Collections.emptyList();
		}
		
		List<GraphSeries> result = processServiceGraph(serviceId, tgInput, 
				serviceIds, timeSpan, transactionGraphs.graphs);
		
		//sortSeriesByName(series);
	
		return result;
	}
    
    private String getSeriesName(TransactionsGraphInput input, 
    		String serviceId, Collection<String> serviceIds,Collection<String> transactions) {
    	
    		String value;
    		boolean useSeriesName;
    		
    		if (input.seriesName != null) {
    			if (input.getAggregateMode() == AggregateMode.Yes) {
    				useSeriesName = true;
    			} else {
        			useSeriesName = CollectionUtil.safeIsEmpty(transactions);
    			}
    		} else {
    			useSeriesName = false;
    		}
    		
    		if (useSeriesName) {
    			value = input.seriesName;
    		} else {
    			if (transactions != null) {
    				value = String.join(ARRAY_SEPERATOR_RAW, transactions);
    			} else {
    				value = input.volumeType.toString();
    			}
    		}
    		
    		String result = getSeriesName(input, value, serviceId, serviceIds); 
    		
    		return result;
    }
   
	protected List<GraphSeries> processServiceGraph(String serviceId, 
			TransactionsGraphInput input, Collection<String> serviceIds,
			Pair<DateTime, DateTime> timespan,  Collection<TransactionGraph> graphs) {
    	
		List<GraphSeries> result;

		GroupFilter transactionsFilter = getTransactionsFilter(serviceId, input, timespan);

		AggregateMode aggregateMode = input.getAggregateMode();
		
		if ((aggregateMode == AggregateMode.Yes) || (!input.hasTransactions())) { 
			
			String seriesName = getSeriesName(input, serviceId, serviceIds, 
				null);
			
			result = Collections.singletonList(createAggregateGraphSeries(serviceId, graphs, transactionsFilter,
					input, serviceIds, seriesName));
			
		} else if (aggregateMode == AggregateMode.No) {
			
			result = createMultiGraphSeries(serviceId, graphs, input, 
				transactionsFilter, serviceIds);
			
		} else if (aggregateMode == AggregateMode.Auto) { 
			
			Collection<String> transactions = input.getTransactions(serviceId);
			
			List<String> transactionGroups = new ArrayList<String>(transactions.size());
			List<String> singleTransactions = new ArrayList<String>(transactions.size());
					
			for (String transaction : transactions) {
				
				if (GroupSettings.isGroup(transaction)) {
					transactionGroups.add(transaction);	
				} else {
					singleTransactions.add(transaction);
				}
			}
			
			result = new ArrayList<GraphSeries>();
			
			for (String transactionGroup : transactionGroups) {
				
				GroupFilter groupsFilter = getTransactionsFilter(serviceId, input, timespan,
					Collections.singletonList(transactionGroup));
				
				if (groupsFilter == null) {
					continue;
				}
				
				String seriesName = getSeriesName(input, serviceId, serviceIds, 
					Collections.singleton(GroupSettings.fromGroupName(transactionGroup)));
				
				result.add(createAggregateGraphSeries(serviceId, graphs, groupsFilter,
						input, serviceIds,seriesName));
			}
			
			if (singleTransactions.size() > 0) {
				
				GroupFilter singleTransactionsFilter = getTransactionsFilter(serviceId, input, timespan,
					singleTransactions);
				
				if ((singleTransactionsFilter != null) && (!singleTransactionsFilter.isEmpty())) {
					result.addAll(createMultiGraphSeries(serviceId, graphs, input, 
							singleTransactionsFilter, serviceIds));
				}
			}
		} else {
			throw new IllegalStateException(String.valueOf(aggregateMode));
		}
		
		return result;
    }
    
	protected List<GraphSeries> createMultiGraphSeries(String serviceId, Collection<TransactionGraph> graphs,
			TransactionsGraphInput input, GroupFilter transactionFilter, Collection<String> serviceIds) {

		List<GraphSeries> result = new ArrayList<GraphSeries>();
		
		for (TransactionGraph graph : graphs) {

			Pair<String, String> nameAndMethod = getFullNameAndMethod(graph.name);
			
			if (filterTransaction(transactionFilter, input.searchText, nameAndMethod.getFirst(), nameAndMethod.getSecond()))
			{
				continue;
			}
			
			if (input.volumeType.equals(GraphType.all)) {
				result.add(createTransactionGraphSeries(serviceId, graph, GraphType.avg_time, serviceIds, input));
				result.add(createTransactionGraphSeries(serviceId, graph, GraphType.invocations, serviceIds, input));
			} else {
				result.add(createTransactionGraphSeries(serviceId, graph, input.volumeType, serviceIds, input));
			}
		}
		
		return result;
	}
	
	private SeriesVolume getAvgSeriesValues(Collection<TransactionGraph> graphs,
			GroupFilter transactionsFilter, TransactionsGraphInput input) {
		
		Collection<TransactionGraph>  targetGraphs;
		String searchText = input.getSearchText();

		if ((transactionsFilter != null) || (searchText != null)) {
			
			targetGraphs = new ArrayList<TransactionGraph>(graphs.size());
			
			for (TransactionGraph graph : graphs) {
		
				Pair<String, String> nameAndMethod = getFullNameAndMethod(graph.name);
				
				if (filterTransaction(transactionsFilter, searchText, nameAndMethod.getFirst(), nameAndMethod.getSecond()))
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
			
		long volume = 0;
		
		for (Map.Entry<Long, TimeAvg> entry : sortedAvgMap.entrySet()) {
			
			Long time = entry.getKey();
			TimeAvg timeAvg = entry.getValue();
						
			volume += timeAvg.invocations;
			Object timeValue = getTimeValue(time, input);
	
			result.add(Arrays.asList(new Object[] { timeValue, Double.valueOf(timeAvg.avgTime) }));
		}
		
		return SeriesVolume.of(result, volume);
	}

	private SeriesVolume getInvSeriesValues(Collection<TransactionGraph> graphs,
			GroupFilter transactionFilter, TransactionsGraphInput input) {

		Map<Long, Long> values = new TreeMap<Long, Long>();
		
		String searchText = input.getSearchText();
		
		for (TransactionGraph graph : graphs) {
			
			Pair<String, String> nameAndMethod = getFullNameAndMethod(graph.name);
			
			if (filterTransaction(transactionFilter, searchText, nameAndMethod.getFirst(), nameAndMethod.getSecond()))
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
			
			Object timeValue = getTimeValue(time, input);

			result.add(Arrays.asList(new Object[] { timeValue, value }));
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
			seriesValues = getAvgSeriesValues(graphs, transactionFilter, input);

		} else {
			seriesValues = getInvSeriesValues(graphs, transactionFilter, input);
		}

		String cleanSeriesName = cleanSeriesName(seriesName);
		
		series.name = EMPTY_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, cleanSeriesName });
		series.values = seriesValues.values;

		return GraphSeries.of(series, seriesValues.volume, cleanSeriesName);

	}

	private GraphSeries createTransactionGraphSeries(String serviceId, TransactionGraph graph, 
			GraphType volumeType, Collection<String> serviceIds, TransactionsGraphInput input) {

		Series series = new Series();
		
		String tagName = getServiceValue(getTransactionName(graph.name, true), serviceId, serviceIds);
		String cleanTagName = cleanSeriesName(tagName);

		series.name = EMPTY_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, cleanTagName });

		SeriesVolume seriesData = processGraphPoints(graph, volumeType, input);

		series.values = seriesData.values;

		return GraphSeries.of(series, seriesData.volume, cleanTagName);
	}
	
	private SeriesVolume processGraphPoints(TransactionGraph graph,
		GraphType volumeType, TransactionsGraphInput input) {

		double volume = 0;
		
		Map<DateTime, Double> values = new TreeMap<DateTime, Double>();
		
		for (GraphPoint gp : graph.points) {
		
			double value;

			if (volumeType.equals(GraphType.avg_time)) {
				value = gp.stats.avg_time;
			} else {
				value = gp.stats.invocations;
			}
			
			DateTime gpTime = ISODateTimeFormat.dateTimeParser().parseDateTime(gp.time);

			volume += value;
						
			Double existingValue = values.get(gpTime);
			
			if (existingValue != null) {
				values.put(gpTime, existingValue.doubleValue() + value);
			} else {
				values.put(gpTime, value);

			}
		}
		
		List<List<Object>> points = new ArrayList<List<Object>>(values.size());
		
		for (Map.Entry<DateTime, Double> entry : values.entrySet()) {
			
			Object timeValue = getTimeValue(entry.getKey().getMillis(), input);
			points.add(Arrays.asList(new Object[] {timeValue, entry.getValue() }));
		}

		return SeriesVolume.of(points, Long.valueOf((long)volume));
	}
	
	@Override
	protected List<Series> processSeries(List<GraphSeries> series, BaseGraphInput input)
	{
		TransactionsGraphInput tgInput = (TransactionsGraphInput)input; 
		
		if (tgInput.limit == 0) {
			return super.processSeries(series, input);
		}
		
		return limitSeries(series, tgInput.limit);
	}
	

	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof TransactionsGraphInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		TransactionsGraphInput input = getInput((TransactionsGraphInput)functionInput);
		
		return super.process(input);
	}
}
