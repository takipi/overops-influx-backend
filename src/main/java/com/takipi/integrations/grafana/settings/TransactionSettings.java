package com.takipi.integrations.grafana.settings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.takipi.integrations.grafana.functions.GrafanaFunction;

public class TransactionSettings {
	public String keyTransactions;
	
	public Collection<String> getKeyTransactions() {
		
		if (keyTransactions == null) {
			return Collections.emptyList();
		}

		String[] types = keyTransactions.split(GrafanaFunction.ARRAY_SEPERATOR);
		return Arrays.asList(types);
	}
}
