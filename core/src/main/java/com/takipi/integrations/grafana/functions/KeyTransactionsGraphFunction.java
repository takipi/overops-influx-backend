package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.util.regression.settings.GroupSettings;
import com.takipi.api.client.util.regression.settings.GroupSettings.Group;
import com.takipi.api.client.util.regression.settings.GroupSettings.GroupFilter;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.TransactionsGraphInput;
import com.takipi.integrations.grafana.input.TransactionsGraphInput.AggregateMode;

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
			Collection<String> serviceIds, Pair<DateTime, DateTime> timespan,  
			Collection<TransactionGraph> graphs)
	{
		if (!input.hasTransactions()) {
			return Collections.emptyList();
		}
		
		GroupSettings transactionGroups = getSettings(serviceId).transactions;
		
		if (transactionGroups == null) {
			return super.processServiceGraph(serviceId, input, serviceIds, timespan, graphs);
		}
	
		List<GraphSeries> result;

		if (input.getAggregateMode() == AggregateMode.Yes) { 
			
			result = new ArrayList<GraphSeries>();
			
			for (Group group : transactionGroups.getGroups()) {
				
				GroupFilter groupFilter = group.getFilter();
				String serviceValue = getServiceValue(group.name, serviceId, serviceIds);
				
				GraphSeries groupSeries = createAggregateGraphSeries(serviceId, graphs, groupFilter,
						input, serviceIds, serviceValue);
				
				if (groupSeries.volume > 0) {
					result.add(groupSeries);
				}
			}
		} else {
			GroupFilter groupFilter = getTransactionsFilter(serviceId, input, timespan);
			result = createMultiGraphSeries(serviceId, graphs, input, groupFilter, serviceIds);
		}
		
		if (input.limit > 0) {
			return result.subList(0, Math.min(input.limit, result.size()));
		}
		
		return result;
	}
	
}
