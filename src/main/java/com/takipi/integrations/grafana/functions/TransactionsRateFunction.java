package com.takipi.integrations.grafana.functions;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.TransactionsVolumeInput;

public class TransactionsRateFunction extends TransactionsVolumeFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsRateFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionsVolumeInput.class;
		}

		@Override
		public String getName() {
			return "transactionsRate";
		}
	}
	
	public TransactionsRateFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	@Override
	protected EventVolume getTransactionVolume(TransactionsVolumeInput input, 
		Pair<DateTime, DateTime> timeSpan ) {
			
		EventVolume eventVolume = getEventVolume(input, VolumeType.hits, timeSpan);
		EventVolume transactionVolume = super.getTransactionVolume(input, timeSpan);
		
		EventVolume result = new EventVolume();
		
		if (transactionVolume.sum > 0) {
			result.sum = eventVolume.sum / transactionVolume.sum ;
		}
		
		return result;
	}
}
