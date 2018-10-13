package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.transaction.TransactionGraph;
import com.takipi.common.api.data.transaction.TransactionGraph.GraphPoint;
import com.takipi.common.api.request.transaction.TransactionsGraphRequest;
import com.takipi.common.api.result.transaction.TransactionsGraphResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.common.api.util.Pair;
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

	public TransactionsGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

	protected List<GraphSeries> processServiceGraph(String serviceId, String viewId, String viewName,
			BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, boolean multiService, int pointsWanted) {

		TransactionsGraphInput input = (TransactionsGraphInput) request;

		Pair<String, String> fromTo =TimeUtils.toTimespan(timeSpan);
		
		TransactionsGraphRequest.Builder builder = TransactionsGraphRequest.newBuilder().setServiceId(serviceId)
				.setViewId(viewId).setFrom(fromTo.getFirst()).setTo(fromTo.getSecond())
				.setWantedPointCount(pointsWanted);
		
		applyFilters(request, serviceId, builder);

		Response<TransactionsGraphResult> response = apiClient.get(builder.build());

		validateResponse(response);
		
		if (response.data == null) {

			return Collections.emptyList();
		}

		Collection<String> transactions;

		if (input.transactions != null) {
			transactions = input.getTransactions(serviceId);
		} else {
			transactions = null;
		}

		List<GraphSeries> result;

		if (input.aggregate) {
			result = Collections.singletonList(createAggregateGraphSeries(serviceId, response.data.graphs, transactions,
					input.volumeType, multiService));
		} else {
			result = createMultiGraphSeries(serviceId, response.data.graphs, input.volumeType, multiService,
					transactions);
		}

		return result;
	}

	private List<GraphSeries> createMultiGraphSeries(String serviceId, List<TransactionGraph> graphs,
			VolumeType volumeType, boolean multiService, Collection<String> transactions) {

		List<GraphSeries> result = new ArrayList<GraphSeries>();

		for (TransactionGraph graph : graphs) {

			String entryPoint = getSimpleClassName(graph.name);

			if ((transactions != null) && (!transactions.contains(entryPoint))) {
				continue;
			}

			if (volumeType.equals(VolumeType.all)) {
				result.add(createTransactionGraphSeries(serviceId, graph, VolumeType.avg_time, multiService));
				result.add(createTransactionGraphSeries(serviceId, graph, VolumeType.invocations, multiService));
			} else {
				result.add(createTransactionGraphSeries(serviceId, graph, volumeType, multiService));

			}
		}

		return result;
	}

	private SeriesVolume getAvgSerivesValues(List<TransactionGraph> graphs,
			Collection<String> transactions) {
		
		Map<Long, Double> values = new TreeMap<Long, Double>();

		for (TransactionGraph graph : graphs) {

			String entryPoint = getSimpleClassName(graph.name);

			if ((transactions != null) && (!transactions.contains(entryPoint))) {
				continue;
			}

			for (GraphPoint gp : graph.points) {
				DateTime gpTime = ISODateTimeFormat.dateTimeParser().parseDateTime(gp.time);
				Long epochTime = Long.valueOf(gpTime.getMillis());

				Double value = values.get(epochTime);

				if (value == null) {
					value = Double.valueOf(gp.stats.avg_time);
				} else {
					value = value + Double.valueOf(gp.stats.avg_time);
				}
				values.put(epochTime, value);
			}
		}

		List<List<Object>> result = new ArrayList<List<Object>>(values.size());

		for (Map.Entry<Long, Double> entry : values.entrySet()) {
			Long time = entry.getKey();
			Double avgSum = entry.getValue();

			result.add(Arrays.asList(new Object[] { time, avgSum / values.size() }));
		}

		return SeriesVolume.of(result, 0L);
	}

	private SeriesVolume getInvSerivesValues(List<TransactionGraph> graphs,
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
			Collection<String> transactions, VolumeType volumeType, boolean multiService) {

		Series series = new Series();

		String tagName;
		
		if (multiService) {
			tagName = volumeType.toString() + SERVICE_SEPERATOR + serviceId;
		} else {
			tagName = volumeType.toString();
		}

		SeriesVolume seriesValues;

		if (volumeType.equals(VolumeType.avg_time)) {
			seriesValues = getAvgSerivesValues(graphs, transactions);

		} else {
			seriesValues = getInvSerivesValues(graphs, transactions);
		}

		series.name = EMPTY_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, tagName });
		series.values = seriesValues.values;

		return GraphSeries.of(series, seriesValues.volume);

	}

	private GraphSeries createTransactionGraphSeries(String serviceId, TransactionGraph graph, VolumeType volumeType,
			boolean multiService) {

		Series series = new Series();

		if (multiService) {
			series.name = getSimpleClassName(graph.name) + SERVICE_SEPERATOR + serviceId;
		} else {
			series.name = getSimpleClassName(graph.name);
		}

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
