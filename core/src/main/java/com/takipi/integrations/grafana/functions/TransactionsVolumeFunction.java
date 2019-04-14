package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseVolumeInput.AggregationType;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsVolumeInput;
import com.takipi.integrations.grafana.input.TransactionsVolumeInput.TransactionVolumeType;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.TimeUtil;

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
	
	private TransactionVolume getTransactionVolumes(String serviceId, Pair<DateTime, DateTime> timeSpan, String viewId,
			TransactionsVolumeInput input) {

		TransactionVolume result = new TransactionVolume();

		long transactionTotal = 0;

		Collection<TransactionGraph> transactionGraphs = getTransactionGraphs(input, serviceId, viewId, 
			timeSpan, input.getSearchText());
		
		if (transactionGraphs == null) {
			return result;
		}
		
		if ((input.volumeType.equals(TransactionVolumeType.avg)) 
		|| (input.volumeType.equals(TransactionVolumeType.invocations))) {

			for (TransactionGraph transactionGraph : transactionGraphs) {
				
				if (transactionGraph.points == null) {
					continue;
				}
				
				for (GraphPoint gp : transactionGraph.points) {
					transactionTotal += gp.stats.invocations;
				}
			}

			result.invocations = transactionTotal;

			if ((transactionTotal > 0) && (input.volumeType.equals(TransactionVolumeType.avg))) {
				
				for (TransactionGraph transactionGraph : transactionGraphs) {
					
					if (transactionGraph.points == null) {
						continue;
					}
					
					for (GraphPoint gp : transactionGraph.points) {
						double rate = (double) gp.stats.invocations / (double) (transactionTotal);
						result.avgTime += gp.stats.avg_time * rate;
					}
				}
			}
		}

		return result;
	}
	
	protected EventVolume getTransactionVolume(TransactionsVolumeInput input, Pair<DateTime, DateTime> timeSpan ) {
		Collection<String> serviceIds = getServiceIds(input);

		long totalInvocations = 0;
		List<TransactionVolume> servicesVolumes = new ArrayList<TransactionVolume>(serviceIds.size());

		for (String serviceId : serviceIds) {

			String viewId = getViewId(serviceId, input.view);
			
			if (viewId == null) {
				continue;
			}
			
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
				
				if (totalInvocations > 0) {
					volume.sum += serviceVolume.avgTime * (serviceVolume.invocations / totalInvocations);
				}
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

		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		
		EventVolume volume = getTransactionVolume(input, timeSpan);
		
		return createSeries(input, timeSpan, volume, AggregationType.sum);
	}
}
