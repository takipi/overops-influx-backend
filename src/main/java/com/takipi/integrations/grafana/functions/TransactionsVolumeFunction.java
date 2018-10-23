package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.transaction.Transaction;
import com.takipi.common.api.util.Pair;
import com.takipi.integrations.grafana.input.BaseVolumeInput.AggregationType;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsVolumeInput;
import com.takipi.integrations.grafana.input.TransactionsVolumeInput.TransactionVolumeType;
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

	private class TransactionVolume {
		protected double avgTime;
		protected long invocations;
	}
	
	private TransactionVolume getTransactionVolumes(String serviceId, Pair<String, String> timeSpan, String viewId,
			TransactionsVolumeInput input) {

		TransactionVolume result = new TransactionVolume();

		long transactionTotal = 0;

		 Collection<Transaction> transactions = getTransactions(serviceId, viewId, timeSpan, input);
		
		if ((input.volumeType.equals(TransactionVolumeType.avg)) || (input.volumeType.equals(TransactionVolumeType.invocations))) {

			for (Transaction transaction : transactions) {
				transactionTotal += transaction.stats.invocations;
			}

			result.invocations = transactionTotal;

			if (input.volumeType.equals(TransactionVolumeType.avg)) {
				for (Transaction transaction : transactions) {
					double rate = (double) transaction.stats.invocations / (double) (transactionTotal);
					result.avgTime += transaction.stats.avg_time * rate;
				}
			}
		}

		return result;
	}
	
	protected EventVolume getTransactionVolume(TransactionsVolumeInput input, Pair<String, String> timeSpan ) {
		String[] serviceIds = getServiceIds(input);

		long totalInvocations = 0;
		List<TransactionVolume> servicesVolumes = new ArrayList<TransactionVolume>(serviceIds.length);

		for (String serviceId : serviceIds) {

			String viewId = getViewId(serviceId, input.view);
			TransactionVolume serviceVolume = getTransactionVolumes(serviceId, timeSpan, viewId, input);

			if (serviceVolume != null) {
				servicesVolumes.add(serviceVolume);
				totalInvocations += serviceVolume.invocations;
			}
		}
		
		EventVolume volume = new EventVolume();

		for (TransactionVolume serviceVolume : servicesVolumes) {

			switch (input.volumeType) {
			case invocations:
				volume.sum += serviceVolume.invocations;
				break;

			case avg:
				volume.sum += serviceVolume.avgTime * (double) serviceVolume.invocations / (double) totalInvocations;
				break;

			case count:
				volume.sum++;
				break;

			}
		}

		return volume;
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
		
		EventVolume volume = getTransactionVolume(input, timeSpan);
		
		return createSeries(input, timeSpan, volume, AggregationType.sum);
	}
}
