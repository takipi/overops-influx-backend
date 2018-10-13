package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.request.ViewTimeframeRequest;
import com.takipi.common.api.request.event.EventsRequest;
import com.takipi.common.api.request.event.EventsVolumeRequest;
import com.takipi.common.api.result.event.EventResult;
import com.takipi.common.api.result.event.EventsResult;
import com.takipi.common.api.result.event.EventsVolumeResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.common.api.util.Pair;
import com.takipi.integrations.grafana.input.BaseVolumeInput.AggregationType;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.VolumeInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class VolumeFunction extends BaseVolumeFunction {

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

	private void applyBuilder(ViewTimeframeRequest.Builder builder, String serviceId, String viewId,
			VolumeInput request, Pair<String, String> timeSpan) {

		builder.setViewId(viewId).setServiceId(serviceId).setFrom(timeSpan.getFirst()).setTo(timeSpan.getSecond());

		applyFilters(request, serviceId, builder);
	}
	
	private EventVolume processServiceVolume(String serviceId, VolumeInput request, Pair<String, String> timeSpan) {

		EventVolume result = new EventVolume();

		String viewId = getViewId(serviceId, request.view);

		if (viewId == null) {
			return result;
		}

		List<EventResult> events;

		if (request.type.equals(AggregationType.count)) {
			EventsRequest.Builder builder = EventsRequest.newBuilder();
			applyBuilder(builder, serviceId, viewId, request, timeSpan);
			Response<EventsResult> eventResponse = apiClient.get(builder.build());
			validateResponse(eventResponse);
			events = eventResponse.data.events;
		} else {
			EventsVolumeRequest.Builder builder = EventsVolumeRequest.newBuilder().setVolumeType(request.volumeType);
			applyBuilder(builder, serviceId, viewId, request, timeSpan);
			Response<EventsVolumeResult> volumeResponse = apiClient.get(builder.build());
			validateResponse(volumeResponse);
			events = volumeResponse.data.events;
		}

		if (events == null) {
			return result;
		}
		
		Collection<String> types = request.getTypes();
		Collection<String> introducedBy = request.getIntroducedBy(serviceId);

		for (EventResult event : events) {
			
			if (filterEvent(types, introducedBy, event)) {
				continue;
			}

			if (event.stats != null) {
				switch (request.volumeType) {
				case invocations:
					result.sum += event.stats.invocations;
					break;

				case hits:
				case all:
					result.sum += event.stats.hits;
					break;
				}
			}
			result.count++;
		}

		return result;
	}

	@Override
	public  List<Series> process(FunctionInput functionInput) {
		
		super.process(functionInput);
		
		if (!(functionInput instanceof VolumeInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		VolumeInput request = (VolumeInput) functionInput;
		
		if ((request.volumeType == null)) {
			throw new IllegalArgumentException("VolumeType");
		}

		Pair<String, String> timeSpan = TimeUtils.parseTimeFilter(request.timeFilter);

		String[] services = getServiceIds(request);

		EventVolume volume = new EventVolume();

		for (String serviceId : services) {
			EventVolume serviceVolume = processServiceVolume(serviceId, request, timeSpan);

			volume.sum = volume.sum + serviceVolume.sum;
			volume.count = volume.count + serviceVolume.count;
		}

		return createSeries(request, timeSpan, volume);
	}
}
