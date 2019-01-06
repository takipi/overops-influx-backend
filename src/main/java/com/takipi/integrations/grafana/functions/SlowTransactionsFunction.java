package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.util.performance.calc.PerformanceState;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.TransactionsListFunction.TransactionData;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.SlowTransactionsInput;
import com.takipi.integrations.grafana.input.TransactionsListInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.TimeUtil;

public class SlowTransactionsFunction extends EnvironmentVariableFunction
{

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new SlowTransactionsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return SlowTransactionsInput.class;
		}

		@Override
		public String getName() {
			return "slowTransactions";
		}
	}
	
	public SlowTransactionsFunction(ApiClient apiClient)
	{
		super(apiClient);
	}
	
	private Collection<String> getSlowTransactions(String serviceId, 
			SlowTransactionsInput input, Pair<DateTime, DateTime> timeSpan)
	{				
		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null) {
			return Collections.emptyList();
		}
		
		Set<String> result = new HashSet<String>();

		Collection<PerformanceState> performanceStates = TransactionsListInput.getStates(input.performanceStates);
		
		Collection<TransactionGraph> activeGraphs = getTransactionGraphs(input, serviceId, 
				viewId, timeSpan, input.getSearchText(), input.pointsWanted, 0, 0);
		
		TransactionsListFunction transactionsFunction = new TransactionsListFunction(apiClient);
		
		Map<Pair<String, String>, TransactionData> transactionDatas = transactionsFunction.getTransactionDatas(activeGraphs,
			serviceId, viewId, timeSpan, input, false, 0);
						
		for (TransactionData transactionData : transactionDatas.values()) {
			
			if (!performanceStates.contains(transactionData.state)) {
				continue;
			}
			
			String name = getTransactionName(transactionData.graph.name, false);
			result.add(name);
		}
		
		return result;
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		if (!(functionInput instanceof SlowTransactionsInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
	}

	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender)
	{
		SlowTransactionsInput stInput = (SlowTransactionsInput)input;
	
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(stInput.timeFilter);
		Collection<String> transactions = getSlowTransactions(serviceId, stInput, timespan);
				
		for (String transaction : transactions) {
			String value = getServiceValue(transaction, serviceId, serviceIds);
			appender.append(value);
		}
	}
	
}
