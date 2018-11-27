package com.takipi.integrations.grafana.input;

public class SlowTransactionGraphInput extends TransactionsGraphInput {
	
	public enum RenderMode {
		AvgTime, SlowCalls
	}
	
	public int limit;
}
