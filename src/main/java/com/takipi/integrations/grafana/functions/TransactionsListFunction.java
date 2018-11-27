package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.Transaction;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsListIput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.EventLinkEncoder;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TransactionsListFunction extends GrafanaFunction {

	private static final List<String> FIELDS = Arrays.asList(new String[] { 
			"Link", "Transaction", "Total", "Avg Response(ms)", "Slow %", "Slow Transactions",
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

	protected class TransactionData {
		protected Transaction transaction;
		protected long timersHits;
		protected long errorsHits;
		protected EventResult currTimer;
	}

	private List<List<Object>> processServiceTransactions(String serviceId, Pair<DateTime, DateTime> timeSpan,
			TransactionsListIput input) {

		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null) {
			return Collections.emptyList();
		}

		Map<String, TransactionData> transactions = getTransactions(serviceId, timeSpan, viewId, input);
		updateTransactionEvents(serviceId, timeSpan, input, transactions);

		if (transactions == null) {
			return Collections.emptyList();
		}
		 
		List<List<Object>> result = formatResultObjects(transactions, serviceId, timeSpan, input);
		
		return result;	
	}
	
	private List<List<Object>> formatResultObjects(Map<String, TransactionData> transactions, 
			String serviceId, Pair<DateTime, DateTime> timeSpan,
			TransactionsListIput input) {
		
		List<List<Object>> result = new ArrayList<List<Object>>(transactions.size());

		for (TransactionData transaction : transactions.values()) {

			String name = getTransactionName(transaction.transaction.name, true);
			
			double errorRate;
			long invocations = transaction.transaction.stats.invocations;
			
			if (invocations > 0) {
				errorRate = (double)transaction.errorsHits / (double)invocations;
			} else {
				errorRate = 0;
			}
			
			double timerRate;
						
			if (invocations > 0) {
				timerRate = (double)transaction.timersHits / (double)invocations;
			} else {
				timerRate = 0;	
			}

			
			String link;
			
			if (transaction.currTimer != null) {
				link = EventLinkEncoder.encodeLink(apiClient, serviceId, input, transaction.currTimer, 
					timeSpan.getFirst(), timeSpan.getSecond());
			} else {
				link = null;
			}
			
			Object[] object = new Object[] { link, name, invocations,
					Long.valueOf((long)transaction.transaction.stats.avg_time), timerRate, 
					transaction.timersHits, errorRate, transaction.errorsHits };
			
			result.add(Arrays.asList(object));
		}

		return result;
	}
	
	private void updateTransactionEvents(String serviceId, Pair<DateTime, DateTime> timeSpan,
			TransactionsListIput input, Map<String, TransactionData> transactions) 
	{
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timeSpan.getFirst(),
			timeSpan.getSecond(), VolumeType.hits, input.pointsWanted);
		
		if (eventsMap == null) {
			return;
		}

		EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);
		
		for (EventResult event : eventsMap.values()) {

			if (event.entry_point == null) {
				continue;
			}
				
			TransactionData transaction = transactions.get(event.entry_point.class_name);

			if (transaction == null) {
				
				transaction = transactions.get(toTransactionName(event.entry_point));
				
				if (transaction == null) {
					continue;
				}
			}
			if (eventFilter.filter(event)) {
				continue;
			}

			if (event.type.equals(TIMER)) {
				transaction.timersHits += event.stats.hits;
				
				if (transaction.currTimer == null) {
					transaction.currTimer = event;
				} else {
					DateTime evrntFirstSeen = TimeUtil.getDateTime(event.first_seen);
					DateTime timerFirstSeen = TimeUtil.getDateTime(transaction.currTimer.first_seen);

					
					long eventDelta = timeSpan.getSecond().getMillis() - evrntFirstSeen.getMillis();
					long timerDelta = timeSpan.getSecond().getMillis() - timerFirstSeen.getMillis();

					if (eventDelta < timerDelta) {
						transaction.currTimer = event;
					}				
				}	
			} else {
				transaction.errorsHits += event.stats.hits;
			}
		}
	}
	
	private Map<String, TransactionData> getTransactions(String serviceId, Pair<DateTime, DateTime> timeSpan,
			String viewId, TransactionsListIput input) {
				
		Collection<Transaction> transactions = getTransactions(serviceId, viewId, timeSpan, input);

		if (transactions == null) {
			return Collections.emptyMap();
		}
				
		Map<String, TransactionData> result = new HashMap<String, TransactionData>();

		for (Transaction transaction :transactions) {	
			TransactionData transactionData = new TransactionData();	
			transactionData.transaction = transaction;
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

		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		Collection<String> serviceIds = getServiceIds(input);

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
