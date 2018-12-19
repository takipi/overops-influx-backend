package com.takipi.integrations.grafana.input;

public class ApiHostInput extends VariableInput
{
	public enum HostType {
		URL, PORT, URL_NO_PORT;
	}
	
	public HostType type;
	
}
