package com.takipi.integrations.grafana.output;

import java.util.List;

public class Series {
	public String name;
	public List<String> tags;
	public List<String> columns;
	public List<List<Object>> values;
}