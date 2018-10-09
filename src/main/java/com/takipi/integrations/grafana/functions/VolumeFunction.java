package com.takipi.integrations.grafana.functions;

import java.util.Arrays;
import java.util.Collections;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.request.volume.EventsVolumeRequest;
import com.takipi.common.api.result.event.EventResult;
import com.takipi.common.api.result.volume.EventsVolumeResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.common.api.util.Pair;
import com.takipi.common.api.util.ValidationUtil.VolumeType;
import com.takipi.integrations.grafana.functions.GroupByFunction.AggregationType;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.input.VolumeInput;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class VolumeFunction extends GrafanaFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new VolumeFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass() {
			return VolumeInput.class;
		}
		
		@Override
		public String getName() {
			return "volume";
		}
	}
	
	
	public VolumeFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	private EventVolume processServiceVolume(String serviceId, GraphInput request,
			Pair<String, String> timeSpan) {

		String viewId = getViewId(serviceId, request);
		
		if (viewId == null) {
			return null;
		}

		EventsVolumeRequest.Builder builder = EventsVolumeRequest.newBuilder().setServiceId(serviceId).setViewId(viewId)
				.setVolumeType(request.volumeType).setFrom(timeSpan.getFirst()).setTo(timeSpan.getSecond());

		applyFilters(request, serviceId, builder);

		Response<EventsVolumeResult> response = apiClient.get(builder.build());

		if ((response.isBadResponse()) || (response.data == null)) {
			throw new IllegalStateException("Volume Result code " + response.responseCode);
		}
		
		EventVolume result = new EventVolume();
		
		if (response.data.events == null) {
			return result ;
		}
				
		for (EventResult event : response.data.events) {
			if (request.volumeType.equals(VolumeType.invocations)) {
				result.sum += event.stats.invocations;
			} else {
				result.sum += event.stats.hits;
			}
			result.count++;
		}
		
		return result;
	}
	
	@Override
	public QueryResult process(FunctionInput functionInput) {
		if (!(functionInput instanceof VolumeInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		VolumeInput request = (VolumeInput)functionInput;
		
		Pair<String, String> timeSpan = TimeUtils.parseTimeFilter(request.timeFilter);

		String[] services = getServiceIds(request);

		Long time = Long.valueOf(TimeUtils.getLongTime(timeSpan.getSecond()));

		EventVolume volume = new EventVolume();
		
		for (String serviceId : services) {
			EventVolume serviceVolume = processServiceVolume(serviceId, request, timeSpan);
			
			if (serviceVolume != null ) {
				volume.sum = volume.sum + serviceVolume.sum;
				volume.count = volume.count + serviceVolume.count;
			}
		}
		
		Series series = new Series();

		series.name = SERIES_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, SUM_COLUMN });

		Object value;

		if (request.type.equals(AggregationType.sum)) {
			value = Long.valueOf(volume.sum);
		} else {
			value = Double.valueOf((double)volume.sum / (double)volume.count);
		}

		series.values = Collections.singletonList(Arrays.asList(new Object[] {time, value}));

		return createQueryResults(Collections.singletonList(series));
	}	
}
