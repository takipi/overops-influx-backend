package com.takipi.integrations.grafana.input;

import com.takipi.common.api.util.ValidationUtil.GraphType;
import com.takipi.common.api.util.ValidationUtil.VolumeType;

public class GraphInput extends ViewInput {
	public GraphType graphType;
	public VolumeType volumeType;
	public long interval;
}