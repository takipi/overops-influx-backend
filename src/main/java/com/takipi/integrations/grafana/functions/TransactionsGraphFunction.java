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
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.GraphFunction.SeriesVolume;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsGraphInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class TransactionsGraphFunction extends BaseGraphFunction {

	public enum VolumeType {
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

	protected List<GraphSeries> processServiceGraph(String serviceId, String viewId, String viewName,
			BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, String[] serviceIds, int pointsWanted) {

		TransactionsGraphInput input = (TransactionsGraphInput) request;

		Pair<String, String> fromTo = TimeUtils.toTimespan(timeSpan);
				
		TransactionsGraphRequest.Builder builder = TransactionsGraphRequest.newBuilder().setServiceId(serviceId)
				.setViewId(viewId).setFrom(fromTo.getFirst()).setTo(fromTo.getSecond())
				.setWantedPointCount(pointsWanted);
				
		applyFilters(request, serviceId, builder);

		Response<TransactionsGraphResult> response = apiClient.get(builder.build());
		
		validateResponse(response);
		
		if ((response.data == null) || (response.data.graphs == null)) { 

			return Collections.emptyList();
		}

		Collection<String> transactions;

		if (input.hasTransactions()) {
			transactions = input.getTransactions(serviceId);
		} else {
			transactions = null;
		}

		List<GraphSeries> result;

		if (input.aggregate) {
			result = Collections.singletonList(createAggregateGraphSeries(serviceId, response.data.graphs, transactions,
					input, serviceIds));
		} else {
			result = createMultiGraphSeries(serviceId, response.data.graphs, input.volumeType, serviceIds,
					transactions);
		}

		return result;
	}

	private List<GraphSeries> createMultiGraphSeries(String serviceId, List<TransactionGraph> graphs,
			VolumeType volumeType, String[] serviceIds, Collection<String> transactions) {

		List<GraphSeries> result = new ArrayList<GraphSeries>();

		for (TransactionGraph graph : graphs) {

			String entryPoint = getSimpleClassName(graph.name);

			if ((transactions != null) && (!transactions.contains(entryPoint))) {
				continue;
			}

			if (volumeType.equals(VolumeType.all)) {
				result.add(createTransactionGraphSeries(serviceId, graph, VolumeType.avg_time, serviceIds));
				result.add(createTransactionGraphSeries(serviceId, graph, VolumeType.invocations, serviceIds));
			} else {
				result.add(createTransactionGraphSeries(serviceId, graph, volumeType, serviceIds));

			}
		}

		return result;
	}
	
	private SeriesVolume getAvgSeriesValues(List<TransactionGraph> graphs,
			Collection<String> transactions) {
		
		List<TransactionGraph>  targetGraphs;
		
		if (transactions != null) {
			
			targetGraphs = new ArrayList<TransactionGraph>(graphs.size());
			
			for (TransactionGraph graph : graphs) {
	
				String entryPoint = getSimpleClassName(graph.name);
	
				if (!transactions.contains(entryPoint)) {
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

	private SeriesVolume getInvSeriesValues(List<TransactionGraph> graphs,
			Collection<String> transactions) {

		Map<Long, Long> values = new TreeMap<Long, Long>();

		for (TransactionGraph graph : graphs) {

			String entryPoint = getSimpleClassName(graph.name);

			if ((transactions != null) && (!transactions.contains(entryPoint))) {
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

	private GraphSeries createAggregateGraphSeries(String serviceId, List<TransactionGraph> graphs,
			Collection<String> transactions, TransactionsGraphInput input, String[] serviceIds) {

		Series series = new Series();
		
		String tagName = getSeriesName(input, input.seriesName, input.volumeType, serviceId, serviceIds);
	
		SeriesVolume seriesValues;

		if (input.volumeType.equals(VolumeType.avg_time)) {
			seriesValues = getAvgSeriesValues(graphs, transactions);

		} else {
			seriesValues = getInvSeriesValues(graphs, transactions);
		}

		series.name = EMPTY_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, tagName });
		series.values = seriesValues.values;

		return GraphSeries.of(series, seriesValues.volume);

	}

	private GraphSeries createTransactionGraphSeries(String serviceId, TransactionGraph graph, VolumeType volumeType,
			String[] serviceIds) {

		Series series = new Series();
		
		series.name = getServiceValue(getSimpleClassName(graph.name), serviceId, serviceIds);
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, volumeType.toString() });

		SeriesVolume seriesData = processGraphPoints(graph, volumeType);

		series.values = seriesData.values;

		return GraphSeries.of(series, seriesData.volume);
	}

	private SeriesVolume processGraphPoints(TransactionGraph graph, VolumeType volumeType) {

		long volume = 0;
		List<List<Object>> values = new ArrayList<List<Object>>(graph.points.size());

		for (GraphPoint gp : graph.points) {
			
			DateTime gpTime = ISODateTimeFormat.dateTimeParser().parseDateTime(gp.time);

			double value;

			if (volumeType.equals(VolumeType.avg_time)) {
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
