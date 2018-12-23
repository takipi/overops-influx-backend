package com.takipi.integrations.grafana.input;

import com.takipi.integrations.grafana.functions.TransactionsGraphFunction.GraphType;

public class TransactionsGraphInput extends BaseGraphInput {
	public GraphType volumeType;
	public boolean aggregate;
	public int limit;
	public String performanceStates;
}
