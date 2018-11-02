package com.takipi.integrations.grafana.input;

import java.util.Collection;

public class ViewInput extends EnvironmentsFilterInput {
	
	public String view;
	public String transactions;
	public String timeFilter;

	public boolean hasTransactions() {
		return hasFilter(transactions);
	}
	
	public Collection<String> getTransactions(String serviceId) {

		if ((transactions == null) || (transactions.length() == 0)) {
			return null;
		}

		return getServiceFilters(transactions, serviceId, true);
	}
}

