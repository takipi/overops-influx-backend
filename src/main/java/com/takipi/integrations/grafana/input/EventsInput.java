package com.takipi.integrations.grafana.input;

import com.takipi.common.api.util.ValidationUtil.VolumeType;

public class EventsInput extends ViewInput {
	public String fields;
	public VolumeType volumeType;
	public int maxColumnLength;
}
