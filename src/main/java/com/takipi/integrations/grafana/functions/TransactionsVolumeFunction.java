package com.takipi.integrations.grafana.functions;

import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.transaction.Transaction;
import com.takipi.common.api.request.transaction.TransactionsVolumeRequest;
import com.takipi.common.api.result.transaction.TransactionsVolumeResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.common.api.util.Pair;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsVolumeInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class TransactionsVolumeFunction extends BaseVolumeFunction {

	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsVolumeFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionsVolumeInput.class;
		}

		@Override
		public String getName() {
			return "transactionsVolume";
		}
	}

	public TransactionsVolumeFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private EventVolume getTransactionVolumes(String serviceId, Pair<String, String> timeSpan,
			String viewId, TransactionsVolumeInput input) {
		
		TransactionsVolumeRequest.Builder builder = TransactionsVolumeRequest.newBuilder().setServiceId(serviceId)
				.setViewId(viewId).setFrom(timeSpan.getFirst()).setTo(timeSpan.getSecond());

		applyFilters(input, serviceId, builder);

		Response<TransactionsVolumeResult> response = apiClient.get(builder.build());

		if (response.isBadResponse()) {
			throw new IllegalStateException(
					"Transnaction volume for service " + serviceId + " code: " + response.responseCode);
		}

		if ((response.data == null) || (response.data.transactions == null)) {
			return null;
		}

		EventVolume result = new EventVolume();
		
		for (Transaction transaction : response.data.transactions) {
			
			switch (input.volumeType) {
			case invocations:
				result.sum += transaction.stats.invocations;
				break;
				
			case avg:
				result.sum += transaction.stats.avg_time;
				break;
				
			case stdDev:
				result.sum += transaction.stats.avg_time_std_deviation;
				break;
			}
		}
		
		result.count = response.data.transactions.size();
		
		return result;
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		super.process(functionInput);
		
		if (!(functionInput instanceof TransactionsVolumeInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		TransactionsVolumeInput input = (TransactionsVolumeInput) functionInput;

		if (input.volumeType == null) {
			throw new IllegalArgumentException("volumeType");
		}
		
		Pair<String, String> timeSpan = TimeUtils.parseTimeFilter(input.timeFilter);
		String[] serviceIds = getServiceIds(input);

		EventVolume volume = new EventVolume();
		
		for (String serviceId : serviceIds) {
			
			String viewId = getViewId(serviceId, input.view);
			EventVolume serviceVolume = getTransactionVolumes(serviceId, timeSpan, viewId, input);
			
			if (serviceVolume != null) {
				volume.sum = volume.sum + serviceVolume.sum;
				volume.count = volume.count + serviceVolume.count;
			}
		}
		
		return createSeries(input, timeSpan, volume);
	}
}
