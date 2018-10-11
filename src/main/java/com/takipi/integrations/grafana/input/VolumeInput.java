package com.takipi.integrations.grafana.input;

import com.takipi.common.api.util.ValidationUtil.GraphType;
import com.takipi.common.api.util.ValidationUtil.VolumeType;
import com.takipi.integrations.grafana.functions.BaseVolumeFunction.AggregationType;

public class VolumeInput extends ViewInput {
	public GraphType graphType;
	public VolumeType volumeType;
	public AggregationType type;
}
