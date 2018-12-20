package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.Transaction;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventFilterInput;
import com.takipi.integrations.grafana.input.TransactionsGraphInput;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TransactionAvgGraphFunction extends TransactionsGraphFunction
{
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionAvgGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionsGraphInput.class;
		}

		@Override
		public String getName() {
			return "transactionsAvgGraph";
		}
	}
	
	public TransactionAvgGraphFunction(ApiClient apiClient)
	{
		super(apiClient);
	}
	
	@Override
	protected Collection<TransactionGraph> getTransactionGraphs(EventFilterInput input, String serviceId,
			String viewId, Pair<DateTime, DateTime> timeSpan, String searchText, 
			int pointsWanted, int activeTimespan, int baselineTimespan)
	{	
		Collection<Transaction> transactions = getTransactions(serviceId, viewId, timeSpan, input, searchText);
		
		if (transactions == null) {
			return Collections.emptyList();
		}
		
		List<TransactionGraph> result = new ArrayList<TransactionGraph>();
		
		for (Transaction transaction : transactions) {
			TransactionGraph transactionGraph = new TransactionGraph();
			transactionGraph.name = transaction.name;
			
			GraphPoint p1 = new GraphPoint();
			p1.time = TimeUtil.toString(timeSpan.getFirst());
			p1.stats = transaction.stats;
			
			GraphPoint p2 = new GraphPoint();
			p2.time = TimeUtil.toString(timeSpan.getSecond());
			p2.stats = transaction.stats;
			
			transactionGraph.points = Arrays.asList(new GraphPoint[] {p1, p2});
			result.add(transactionGraph);
		}
		
		return result;
	}
}
