package com.takipi.integrations.grafana.input;

public class TransactionsVolumeInput extends BaseVolumeInput {
	
	public enum TransactionVolumeType {
		invocations, avg, count;
	}
	
	public enum TransactionFilterType {
		events, timers;
	}
	
	public TransactionVolumeType volumeType;
	public TransactionFilterType filter;
}
