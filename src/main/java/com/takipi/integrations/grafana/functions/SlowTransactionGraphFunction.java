package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.SlowTransactionGraphInput;
import com.takipi.integrations.grafana.input.TransactionsGraphInput;
import com.takipi.integrations.grafana.output.Series;

public class SlowTransactionGraphFunction extends TransactionsGraphFunction
{
	public static class Factory implements FunctionFactory
	{
		
		@Override
		public GrafanaFunction create(ApiClient apiClient)
		{
			return new SlowTransactionGraphFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass()
		{
			return SlowTransactionGraphInput.class;
		}
		
		@Override
		public String getName()
		{
			return "slowTransactionsGraph";
		}
	}
	
	public SlowTransactionGraphFunction(ApiClient apiClient)
	{
		super(apiClient);
	}
	
	@Override
	protected Collection<String> getTransactions(String serviceId, 
		TransactionsGraphInput input, Pair<DateTime, DateTime> timeSpan)
	{
		SlowTransactionsFunction stFunction = new SlowTransactionsFunction(apiClient);
		return stFunction.getSlowTransactions(serviceId, input, timeSpan, ((SlowTransactionGraphInput)input).limit);
	}
	
	@Override
	protected List<GraphSeries> processServiceGraph(String serviceId, 
			TransactionsGraphInput input, Collection<String> serviceIds, Collection<String> transactions,
			Collection<TransactionGraph> graphs) {
    	
		List<GraphSeries> result;

		if ((input.aggregate) || (CollectionUtil.safeIsEmpty(transactions))) { 
			return super.processServiceGraph(serviceId, input, serviceIds, transactions, graphs);
					
		} else {
			
			result = new ArrayList<GraphSeries>();
			
			for (String transaction : transactions) {
				result.add(createAggregateGraphSeries(serviceId, graphs, Collections.singletonList(transaction), 
					input, serviceIds, getTransactionName(transaction, false)));
			}
		}
		
		return result;
    }
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		if (!(functionInput instanceof SlowTransactionGraphInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
	}
	
}
