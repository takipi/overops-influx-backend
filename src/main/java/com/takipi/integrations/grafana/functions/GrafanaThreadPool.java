package com.takipi.integrations.grafana.functions;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class GrafanaThreadPool {
	//needs to be combined into containing server thread pooling
	public static final Executor executor = Executors.newFixedThreadPool(20);
}
