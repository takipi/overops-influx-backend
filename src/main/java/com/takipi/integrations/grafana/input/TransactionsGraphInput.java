package com.takipi.integrations.grafana.input;

import com.takipi.integrations.grafana.functions.TransactionsGraphFunction.VolumeType;

public class TransactionsGraphInput extends BaseGraphInput {
	public VolumeType volumeType;
	public boolean aggregate;
}
