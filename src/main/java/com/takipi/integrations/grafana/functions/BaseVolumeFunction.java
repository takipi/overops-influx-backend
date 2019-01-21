package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseVolumeInput;
import com.takipi.integrations.grafana.input.BaseVolumeInput.AggregationType;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;

public abstract class BaseVolumeFunction extends GrafanaFunction {

	protected static class EventVolume {
		protected double sum;
		protected long count;
	}

	public BaseVolumeFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof BaseVolumeInput)) {
			throw new IllegalArgumentException("GraphInput");
		}

		BaseVolumeInput input = (BaseVolumeInput) functionInput;

		if (AggregationType.valueOf(input.type) == null) {
			throw new IllegalArgumentException("type");
		}

		return null;
	}

	protected List<Series> createSeries(BaseVolumeInput input, Pair<DateTime, DateTime> timeSpan, EventVolume volume,
			AggregationType type) {

		Object value;

		switch (type) {
		case sum:
			value = Double.valueOf(volume.sum);
			break;

		case avg:
			value = Double.valueOf(volume.sum / volume.count);
			break;

		case count:
			value = Long.valueOf(volume.count);
			break;

		default:
			throw new IllegalStateException(input.type);
		}

		return createSingleStatSeries(timeSpan, value);
	}
	
	/**
	 * @param input - needed for children 
	 */
	protected boolean filterEvent(EventResult event, BaseVolumeInput input,
		EventFilter eventFilter) {
		return eventFilter.filter(event);
	}

	private EventVolume processServiceVolume(String serviceId, BaseVolumeInput input, VolumeType volumeType,
			Pair<DateTime, DateTime> timeSpan) {

		EventVolume result = new EventVolume();

		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, 
			timeSpan.getFirst(), timeSpan.getSecond(), volumeType, input.pointsWanted);

		if (eventsMap == null) {
			return result;
		}

		EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);

		Set<String> similarIds = new HashSet<String>();
		
		for (EventResult event : eventsMap.values()) {

			if (filterEvent(event, input, eventFilter)) {
				continue;
			}
			
			if (event.stats != null) {
				switch (volumeType) {
				case invocations:
					result.sum += event.stats.invocations;
					break;

				case hits:
				case all:
					result.sum += event.stats.hits;
					break;
				}

				if ((event.stats.hits > 0) && (!similarIds.contains(event.id))) {
					result.count++;
				}
			}

			if (event.similar_event_ids != null) {
				similarIds.addAll(event.similar_event_ids);
			}
		}

		return result;
	}

	protected EventVolume getEventVolume(BaseVolumeInput input, VolumeType volumeType,
			Pair<DateTime, DateTime> timeSpan) {

		Collection<String> serviceIds = getServiceIds(input);

		EventVolume volume = new EventVolume();

		for (String serviceId : serviceIds) {
			EventVolume serviceVolume = processServiceVolume(serviceId, input, volumeType, timeSpan);

			volume.sum = volume.sum + serviceVolume.sum;
			volume.count = volume.count + serviceVolume.count;
		}

		return volume;
	}
}
