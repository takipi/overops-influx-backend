package com.takipi.integrations.grafana.functions;

import java.util.Collection;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.transaction.Transaction;
import com.takipi.common.api.data.view.SummarizedView;
import com.takipi.common.api.util.Pair;
import com.takipi.common.udf.util.ApiViewUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class TransactionsFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return ViewInput.class;
		}

		@Override
		public String getName() {
			return "transactions";
		}
	}

	public TransactionsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, String[] serviceIds, String serviceId,
			VariableAppender appender) {
		
		ViewInput viewInput = (ViewInput)input;
		Pair<String, String> timespan = TimeUtils.parseTimeFilter(viewInput.timeFilter);

		SummarizedView view = ApiViewUtil.getServiceViewByName(apiClient, serviceId, viewInput.view);
		
		if (view == null) {
			return;
		}
		
		Collection<Transaction> transactions = getTransactions(serviceId, view.id, timespan, viewInput);
		
		if (transactions == null) {
			return;
		}
		
		for (Transaction transaction : transactions) {
			
			String entryPoint = getSimpleClassName(transaction.name);
			String serviceEntryPoint = getServiceValue(entryPoint, serviceId, serviceIds);
			
			appender.append(serviceEntryPoint);			
		}
	}
}
