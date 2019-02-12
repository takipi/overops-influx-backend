package com.takipi.integrations.grafana.functions;

import java.util.List;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseVolumeInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsRateInput;
import com.takipi.integrations.grafana.input.TransactionsRateInput.TransactionFilterType;
import com.takipi.integrations.grafana.input.TransactionsVolumeInput;
import com.takipi.integrations.grafana.output.Series;

public class TransactionsRateFunction extends TransactionsVolumeFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsRateFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionsRateInput.class;
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
		
		if ((TransactionFilterType.events.equals(((TransactionsRateInput)input).filter))) {
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
		
		TransactionsRateInput trInput = (TransactionsRateInput)input;
		
		VolumeType volumeType;
		
		if (trInput.eventVolumeType != null) {
			volumeType = trInput.eventVolumeType;
		} else {
			volumeType = VolumeType.hits;
		}
		
		EventVolume eventVolume = getEventVolume(input, volumeType, timeSpan);
		EventVolume transactionVolume = super.getTransactionVolume(input, timeSpan);
		
		EventVolume result = new EventVolume();
		
		if (transactionVolume.sum > 0) {
			
			double value = eventVolume.sum / transactionVolume.sum ;
			
			if ((trInput.allowExcceed100) && (result.sum > 1)) {
				result.sum = 1f;
			} else {
				result.sum = value;
			}	
		}
		
		return result;
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof TransactionsRateInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		return super.process(functionInput);
	}
}
