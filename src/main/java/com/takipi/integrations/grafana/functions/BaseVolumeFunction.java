package com.takipi.integrations.grafana.functions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.util.Pair;
import com.takipi.integrations.grafana.input.BaseVolumeInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public abstract class BaseVolumeFunction extends GrafanaFunction{
	
	protected static class EventVolume {
		protected long sum;
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

		if (input.type == null) {
			throw new IllegalArgumentException("type");
		}
	
		return null;
	}
	
	protected List<Series> createSeries(BaseVolumeInput request, Pair<String, String> timeSpan, EventVolume volume) {
		Series series = new Series();

		series.name = SERIES_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, SUM_COLUMN });

		Object value;

		switch (request.type) {
		case sum:
			value = Long.valueOf(volume.sum);
			break;

		case avg:
			value = Double.valueOf((double) volume.sum / (double) volume.count);
			break;

		case count:
			value = Long.valueOf(volume.count);
			break;

		default:
			throw new IllegalStateException(String.valueOf(request.type));
		}

		Long time = Long.valueOf(TimeUtils.getLongTime(timeSpan.getSecond()));

		series.values = Collections.singletonList(Arrays.asList(new Object[] { time, value }));

		return Collections.singletonList(series);
	}

}
