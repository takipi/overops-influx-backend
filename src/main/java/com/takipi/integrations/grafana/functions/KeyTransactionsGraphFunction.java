package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.common.util.CollectionUtil;
import com.takipi.integrations.grafana.input.TransactionsGraphInput;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings;
import com.takipi.integrations.grafana.settings.GroupSettings.Group;

public class KeyTransactionsGraphFunction extends TransactionsGraphFunction
{
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new KeyTransactionsGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionsGraphInput.class;
		}

		@Override
		public String getName() {
			return "keyTransactionsGraph";
		}
	}

	public KeyTransactionsGraphFunction(ApiClient apiClient)
	{
		super(apiClient);
	}
	
	@Override
	protected List<GraphSeries> processServiceGraph(String serviceId, TransactionsGraphInput input,
			Collection<String> serviceIds, Collection<String> transactions, Collection<TransactionGraph> graphs)
	{
		GroupSettings transactionGroups = GrafanaSettings.getData(apiClient, serviceId).transactions;
		
		if (transactionGroups == null) {
			return super.processServiceGraph(serviceId, input, serviceIds, transactions, graphs);
		}
	
		List<GraphSeries> result;

		if ((input.aggregate) || (CollectionUtil.safeIsEmpty(transactions))) { 
			
			result = new ArrayList<GraphSeries>();
			
			for (Group group : transactionGroups.getGroups()) {
				GraphSeries groupSeries = createAggregateGraphSeries(serviceId, graphs, group.getFilter(),
						input, serviceIds, group.name);
				
				if (groupSeries.volume > 0) {
					result.add(groupSeries);
				}
			}
		} else {
			result = createMultiGraphSeries(serviceId, graphs, input, serviceIds);
		}
		
		return result;
	}
	
}
