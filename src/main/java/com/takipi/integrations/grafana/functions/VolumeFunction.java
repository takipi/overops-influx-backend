package com.takipi.integrations.grafana.functions;

import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;
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
		EventVolume volume = getEventVolume(request, request.volumeType, timeSpan);

		return createSeries(request, timeSpan, volume, AggregationType.valueOf(request.type));
	}
}
