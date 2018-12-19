package com.takipi.integrations.grafana.input;

import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;

public class TransactionsVolumeInput extends BaseVolumeInput {
	
	public enum TransactionVolumeType {
		invocations, avg, count;
	}
	
	public enum TransactionFilterType {
		events, timers;
	}
	
	public TransactionVolumeType volumeType;
	public VolumeType eventVolumeType;
	public TransactionFilterType filter;
}
