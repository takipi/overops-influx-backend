package com.takipi.integrations.grafana.input;

public class TransactionsVolumeInput extends BaseVolumeInput {
	
	public enum TransactionVolumeType {
		invocations, avg, count;
	}
	
	public TransactionVolumeType volumeType;
}
