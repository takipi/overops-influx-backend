package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.event.Location;
import com.takipi.common.api.data.transaction.Transaction;
import com.takipi.common.api.request.event.EventsVolumeRequest;
import com.takipi.common.api.request.transaction.TransactionsVolumeRequest;
import com.takipi.common.api.result.event.EventResult;
import com.takipi.common.api.result.event.EventsVolumeResult;
import com.takipi.common.api.result.transaction.TransactionsVolumeResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.common.api.util.Pair;
import com.takipi.common.api.util.ValidationUtil.VolumeType;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsListIput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class TransactionsListFunction extends GrafanaFunction {

	private static final List<String> FIELDS = Arrays.asList(new String[] { 
			"Transaction", "Total", "Avg Response(ms)", "Slow %", "Slow Transactions",
			"Error %", "Failed Requests" });

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsListFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionsListIput.class;
		}

		@Override
		public String getName() {
			return "transactionsList";
		}
	}

	public TransactionsListFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private class TransactionData {

		protected Location entryPoint;
		protected long timersHits;
		protected long timersInvocations;
		protected long errorsHits;
		protected long errorsInvocations;
		protected long invocations;
		protected double avgTime;
	}

	private List<List<Object>> processServiceTransactions(String serviceId, Pair<String, String> timeSpan,
			TransactionsListIput input) {

		String viewId = getViewId(serviceId, input.view);

		Map<String, TransactionData> transactions = getTransactionEvents(serviceId, timeSpan, viewId, input);

		if (transactions == null) {
			return Collections.emptyList();
		}
		 
		if (!updateTransactionVolumes(serviceId, timeSpan, viewId, input, transactions)) {
			return Collections.emptyList();
		}
		
		List<List<Object>> result = formatResultObjects(transactions);
		
		return result;	
	}
	
	private List<List<Object>> formatResultObjects(Map<String, TransactionData> transactions) {
		
		List<List<Object>> result = new ArrayList<List<Object>>(transactions.size());

		for (TransactionData transaction : transactions.values()) {

			String name = formatLocation(transaction.entryPoint);

			double errorRate;
			
			if ( transaction.errorsInvocations > 0) {
				errorRate = (double)transaction.errorsHits / (double)transaction.errorsInvocations;
			} else {
				errorRate = 0;
			}
			
			double timerRate;
			
			if (transaction.timersInvocations > 0) {
				timerRate = (double)transaction.timersHits / (double)transaction.timersInvocations;
			} else {
				timerRate = 0;	
			}

			Object[] object = new Object[] { name, transaction.invocations,
					transaction.avgTime, timerRate, transaction.timersHits, errorRate, transaction.errorsHits };

			System.out.println(object);
			
			result.add(Arrays.asList(object));
		}

		return result;
	}
	
	private Map<String, TransactionData> getTransactionEvents(String serviceId, Pair<String, String> timeSpan,
			String viewId, TransactionsListIput input) 
	{
		EventsVolumeRequest.Builder builder = EventsVolumeRequest.newBuilder().setServiceId(serviceId).setViewId(viewId)
				.setFrom(timeSpan.getFirst()).setTo(timeSpan.getSecond()).setVolumeType(VolumeType.all);

		applyFilters(input, serviceId, builder);

		Response<EventsVolumeResult> response = apiClient.get(builder.build());

		validateResponse(response);

		if ((response.data == null) || (response.data.events == null)) {
			return null;
		}

		Map<String, TransactionData> result = new TreeMap<String, TransactionData>();

		EventFilter eventFilter = input.getEventFilter(serviceId);

		for (EventResult event : response.data.events) {

			TransactionData transaction = result.get(event.entry_point.class_name);

			if (transaction == null) {
				transaction = new TransactionData();
				transaction.entryPoint = event.entry_point;
				result.put(event.entry_point.class_name, transaction);
			}
			
			if (eventFilter.filter(event)) {
				continue;
			}

			if (event.type.equals("Timer")) {
				transaction.timersHits += event.stats.hits;
				transaction.timersInvocations += event.stats.invocations;
			} else {
				transaction.errorsHits += event.stats.hits;
				transaction.errorsInvocations += event.stats.invocations;
			}
		}
	
		return result;
	}
	
	private boolean updateTransactionVolumes(String serviceId, Pair<String, String> timeSpan,
			String viewId, TransactionsListIput input, Map<String, TransactionData> transactions) {
		
		TransactionsVolumeRequest.Builder builder = TransactionsVolumeRequest.newBuilder().setServiceId(serviceId)
				.setViewId(viewId).setFrom(timeSpan.getFirst()).setTo(timeSpan.getSecond());

		applyFilters(input, serviceId, builder);

		Response<TransactionsVolumeResult> response = apiClient.get(builder.build());

		validateResponse(response);

		if ((response.data == null) || (response.data.transactions == null)) {
			return false;
		}

		for (Transaction transaction : response.data.transactions) {
			TransactionData transactionData = transactions.get(transaction.name.replace('/', '.'));

			if (transactionData == null) {
				continue;
			}

			transactionData.invocations = transaction.stats.invocations;
			transactionData.avgTime = transaction.stats.avg_time;
		}
		
		return true;

	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof TransactionsListIput)) {
			throw new IllegalArgumentException("functionInput");
		}

		TransactionsListIput input = (TransactionsListIput) functionInput;

		Pair<String, String> timeSpan = TimeUtils.parseTimeFilter(input.timeFilter);
		String[] serviceIds = getServiceIds(input);

		Series series = new Series();
		
		series.name = SERIES_NAME;
		series.columns = FIELDS;
		series.values = new ArrayList<List<Object>>();

		for (String serviceId : serviceIds) {
			List<List<Object>> serviceEvents = processServiceTransactions(serviceId, timeSpan, input);		
			series.values.addAll(serviceEvents);
		}

		return Collections.singletonList(series);
	}
}
