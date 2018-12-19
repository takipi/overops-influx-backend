package com.takipi.integrations.grafana.functions;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseVolumeInput;
import com.takipi.integrations.grafana.input.TransactionsVolumeInput;
import com.takipi.integrations.grafana.input.TransactionsVolumeInput.TransactionFilterType;

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
	protected boolean filterEvent(EventResult event, BaseVolumeInput input,
		EventFilter eventFilter) {
		
		if ((TransactionFilterType.events.equals(((TransactionsVolumeInput)input).filter))) {
			return super.filterEvent(event, input, eventFilter);
		}
		
		if (!TIMER.equals(event.type)) {
			return true;
		}
		
		if (eventFilter.filterTransaction(event) ) {
			return true;
		}
				
		return false;
	}
	
	@Override
	protected EventVolume getTransactionVolume(TransactionsVolumeInput input, 
		Pair<DateTime, DateTime> timeSpan ) {
			
		VolumeType volumeType;
		
		if (input.eventVolumeType != null) {
			volumeType = input.eventVolumeType;
		} else {
			volumeType = VolumeType.hits;
		}
		
		EventVolume eventVolume = getEventVolume(input, volumeType, timeSpan);
		EventVolume transactionVolume = super.getTransactionVolume(input, timeSpan);
		
		EventVolume result = new EventVolume();
		
		if (transactionVolume.sum > 0) {
			result.sum = eventVolume.sum / transactionVolume.sum ;
		}
		
		return result;
	}
}
