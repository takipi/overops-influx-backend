package com.takipi.integrations.grafana.input;

import java.util.Collection;

import com.takipi.integrations.grafana.functions.TransactionsGraphFunction.VolumeType;

public class TransactionsGraphInput extends BaseGraphInput {
	public VolumeType volumeType;
	public String transactions;
	public boolean aggregate;
	
	public Collection<String> getTransactions() {
		
		if (transactions == null) {
			return null;
		}
	
		return getServiceFilters(transactions, null);
	}
}
