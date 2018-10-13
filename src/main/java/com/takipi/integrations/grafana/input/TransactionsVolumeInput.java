package com.takipi.integrations.grafana.input;

public class TransactionsVolumeInput extends BaseVolumeInput {
	
	public enum VolumeType {
		invocations, avg, stdDev;
	}
	
	public VolumeType volumeType;
}
