package com.takipi.integrations.grafana.functions;

import java.util.Collection;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.Transaction;
import com.takipi.api.client.data.view.SummarizedView;
import com.takipi.api.client.util.view.ViewUtil;
import com.takipi.common.util.Pair;
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
		Pair<DateTime, DateTime> timespan = TimeUtils.getTimeFilter(viewInput.timeFilter);

		SummarizedView view = ViewUtil.getServiceViewByName(apiClient, serviceId, viewInput.view);
		
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
