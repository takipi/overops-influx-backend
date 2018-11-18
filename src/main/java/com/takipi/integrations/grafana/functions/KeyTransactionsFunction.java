
package com.takipi.integrations.grafana.functions;

import java.util.Collection;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.TransactionSettings;

public class KeyTransactionsFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new KeyTransactionsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EnvironmentsInput.class;
		}

		@Override
		public String getName() {
			return "keyTransactions";
		}
	}

	public KeyTransactionsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {

		TransactionSettings transactionSettings = GrafanaSettings.getServiceSettings(apiClient, serviceId).transactions;

		if ((transactionSettings != null) && (transactionSettings.keyTransactions != null)) {

			for (String KeyTx : transactionSettings.getKeyTransactions()) {
				String serviceTx = getServiceValue(KeyTx, serviceId, serviceIds);
				appender.append(serviceTx);
			}
		}
	}
}