package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.List;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.Transaction;
import com.takipi.api.client.data.view.SummarizedView;
import com.takipi.api.client.util.view.ViewUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.TransactionSettings;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TransactionsFunction extends EnvironmentVariableFunction {

	public static final String KEY_TRANSACTIONS = "Key Transactions";
	
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
	@SuppressWarnings("unchecked")
	protected int compareValues(Object o1, Object o2) {
		Object a = ((List<Object>) o1).get(1);
		Object b = ((List<Object>) o2).get(1);
		
		if (a.equals(KEY_TRANSACTIONS)) {
			return -1;
		}
		
		if (b.equals(KEY_TRANSACTIONS)) {
			return 1;
		}
		
		return super.compareValues(o1, o2);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {
		
		ViewInput viewInput = (ViewInput)input;
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(viewInput.timeFilter);

		SummarizedView view = ViewUtil.getServiceViewByName(apiClient, serviceId, viewInput.view);
		
		if (view == null) {
			return;
		}
		
		TransactionSettings transactionSettings = GrafanaSettings.getServiceSettings(apiClient, serviceId).transactions;
		
		if ((transactionSettings != null) && (transactionSettings.keyTransactions != null)) {
			
			String keyTx = getServiceValue(KEY_TRANSACTIONS, serviceId, serviceIds);
			appender.append(keyTx);			
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
