package com.takipi.integrations.grafana.input;

import java.util.Collection;

import com.google.common.base.Objects;

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
	
	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj)) {
			return false;
		}
		
		if (!(obj instanceof ViewInput)) {
			return false;
		}
		
		ViewInput other = (ViewInput)obj;
		
		return Objects.equal(other.view, other.view) 
				&& Objects.equal(other.transactions, other.transactions)
				&& Objects.equal(other.timeFilter, other.timeFilter);
	}
	
	@Override
	public int hashCode() {
		
		if (view != null) {
			return super.hashCode() ^ view.hashCode();
		}
		
		return super.hashCode();
	}
}

