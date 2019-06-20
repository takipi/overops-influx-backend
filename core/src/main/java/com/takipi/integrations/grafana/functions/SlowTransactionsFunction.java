package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.util.performance.calc.PerformanceState;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.SlowTransactionsInput;
import com.takipi.integrations.grafana.input.TransactionsListInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.TimeUtil;

public class SlowTransactionsFunction extends TransactionsFunction {

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
	
	public SlowTransactionsFunction(ApiClient apiClient) {
		super(apiClient);
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
			VariableAppender appender) {
		
		SlowTransactionsInput stInput = (SlowTransactionsInput)input;
	
		String viewId = getViewId(serviceId, stInput.view);
		
		if (viewId == null) {
			return;
		}
		
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(stInput.timeFilter);		
		Collection<PerformanceState> performanceStates = TransactionsListInput.getStates(stInput.performanceStates);
		
		Collection<TransactionGraph> activeGraphs = getTransactionGraphs(stInput, serviceId, 
				viewId, timespan, stInput.getSearchText());
		
		TransactionDataResult transactionDataResult = getTransactionDatas(activeGraphs,
			serviceId, viewId, timespan, stInput, true, false, false, true);
			
		if (transactionDataResult == null) {
			return;
		}
		
		Map<String, List<String>> transactionMap = new HashMap<String, List<String>>();

		for (TransactionData transactionData : transactionDataResult.items.values()) {
			
			if (!performanceStates.contains(transactionData.state)) {
				continue;
			}
			
			appendTransaction(transactionData.graph.name, transactionMap);
		}
		
		appendTransactions(serviceIds, serviceId, appender, transactionMap);
	}	
}
