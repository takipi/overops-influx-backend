package com.takipi.integrations.grafana.input;

public class TransactionsListIput extends BaseGraphInput {
	
	public enum RenderMode
	{
		SingleStat,
		Grid
	}
	
	public String fields;
	public RenderMode renderMode;
	public String singleStatFormat;
	
	public String performanceStates;
}
