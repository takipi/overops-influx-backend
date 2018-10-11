package com.takipi.integrations.grafana.functions;

import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.output.Series;

public abstract class BaseVolumeFunction extends GrafanaFunction{

	public enum AggregationType {
		sum, avg, count;
	}
	
	public BaseVolumeFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		if (!(functionInput instanceof GraphInput)) {
			throw new IllegalArgumentException("GraphInput");
		}

		GraphInput request = (GraphInput) functionInput;
		
		if ((request.graphType == null)) {
			throw new IllegalArgumentException("graphType");
		}
		
		if ((request.volumeType == null)) {
			throw new IllegalArgumentException("volumeType");
		}
		
		return null;
	}

}
