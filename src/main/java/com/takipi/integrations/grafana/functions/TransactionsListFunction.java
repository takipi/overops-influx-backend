package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.Transaction;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
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

		protected String entryPoint;
		protected long timersHits;
		protected long errorsHits;
		protected long invocations;
		protected double avgTime;
	}

	private List<List<Object>> processServiceTransactions(String serviceId, Pair<String, String> timeSpan,
			TransactionsListIput input) {

		String viewId = getViewId(serviceId, input.view);

		Map<String, TransactionData> transactions = getTransactions(serviceId, timeSpan, viewId, input);
		updateTransactionEvents(serviceId, timeSpan, viewId, input, transactions);

		if (transactions == null) {
			return Collections.emptyList();
		}
		 
		List<List<Object>> result = formatResultObjects(transactions);
		
		return result;	
	}
	
	private List<List<Object>> formatResultObjects(Map<String, TransactionData> transactions) {
		
		List<List<Object>> result = new ArrayList<List<Object>>(transactions.size());

		for (TransactionData transaction : transactions.values()) {

			String name = getSimpleClassName(transaction.entryPoint);
			
			double errorRate;
			
			if ( transaction.invocations > 0) {
				errorRate = (double)transaction.errorsHits / (double)transaction.invocations;
			} else {
				errorRate = 0;
			}
			
			double timerRate;
			
			if (transaction.invocations > 0) {
				timerRate = (double)transaction.timersHits / (double)transaction.invocations;
			} else {
				timerRate = 0;	
			}

			Object[] object = new Object[] { name, transaction.invocations,
					Long.valueOf((long)transaction.avgTime), timerRate, transaction.timersHits, errorRate, transaction.errorsHits };
			
			result.add(Arrays.asList(object));
		}

		return result;
	}
	
	private void updateTransactionEvents(String serviceId, Pair<String, String> timeSpan,
			String viewId, TransactionsListIput input, Map<String, TransactionData> transactions) 
	{
		Collection<EventResult> events = getEventList(serviceId, input, timeSpan, VolumeType.all);
		
		if (events == null) {
			throw new IllegalStateException("Could not aquire transaction events for " + serviceId);
		}

		EventFilter eventFilter = input.getEventFilter(serviceId);
		
		for (EventResult event : events) {

			TransactionData transaction = transactions.get(event.entry_point.class_name);

			if (transaction == null) {
				continue;
			}
			
			if (eventFilter.filter(event)) {
				continue;
			}

			if (event.type.equals("Timer")) {
				transaction.timersHits += event.stats.hits;
			} else {
				transaction.errorsHits += event.stats.hits;
			}
		}
	}
	
	private Map<String, TransactionData> getTransactions(String serviceId, Pair<String, String> timeSpan,
			String viewId, TransactionsListIput input) {
				
		Collection<Transaction> transactions = getTransactions(serviceId, viewId, timeSpan, input);

		Collection<String> transactionsFilter = input.getTransactions(serviceId);
		
		Map<String, TransactionData> result = new HashMap<String, TransactionData>();

		for (Transaction transaction :transactions) {
			
			String simpleName = getSimpleClassName(transaction.name);
				
			if ((transactionsFilter != null) && (!transactionsFilter.contains(simpleName))) {
				continue;
			}
			
			TransactionData transactionData = new TransactionData();
			
			transactionData.invocations = transaction.stats.invocations;
			transactionData.avgTime = transaction.stats.avg_time;
			transactionData.entryPoint = transaction.name;
			
			result.put(toQualified(transaction.name), transactionData);
		}
		
		return result;

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
