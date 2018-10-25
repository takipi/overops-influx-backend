package com.takipi.integrations.grafana.functions;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.util.Pair;
import com.takipi.common.api.util.ValidationUtil.VolumeType;
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
		Pair<String, String> timeSpan ) {
			
		EventVolume eventVolume = getEventVolume(input, VolumeType.hits, timeSpan);
		EventVolume transactionVolume = super.getTransactionVolume(input, timeSpan);
		
		EventVolume result = new EventVolume();
		
		if (transactionVolume.sum > 0) {
			result.sum = eventVolume.sum / transactionVolume.sum ;
		}
		
		return result;
	}
}
