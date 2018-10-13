package com.takipi.integrations.grafana.input;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.takipi.integrations.grafana.functions.EventFilter;
import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.utils.ArrayUtils;

public class FilterInput extends EnvironmentsInput {
	
	
	public String timeFilter;
	public String applications;
	public String servers;
	public String deployments;
	public String introducedBy;
	public String types;
	public String transactions;
	
	protected Collection<String> getServiceFilters(String value, String serviceId) {
		
		if (GrafanaFunction.VAR_ALL.equals(value)) {
			return Collections.emptySet();
		}
		
		String[] values = ArrayUtils.safeSplitArray(value, GrafanaFunction.GRAFANA_SEPERATOR, false);
		Set<String> result = new HashSet<String>();
		
		for (int i= 0; i < values.length; i++) {
			String service = values[i];
			String clean = service.replace("(", "").replace(")", "");
			
			String[] parts = clean.split(GrafanaFunction.SERVICE_SEPERATOR);
				
			if ((serviceId != null) && (parts.length > 1)) {
				if (!serviceId.equals(parts[1])) {
					continue;
				}
				
				result.add(parts[0]);
			} else {
				result.add(clean);
			}
		}
		
		return result;
	}
	
	private boolean hasFilter(String value) {
		return (value != null) && (value.length() != 0) && (!value.equals(GrafanaFunction.VAR_ALL));

	}
	
	public boolean hasApplications() {
		return hasFilter(applications);
	}
	
	public boolean hasServers() {
		return hasFilter(servers);
	}
	
	public boolean hasDeployments() {
		return hasFilter(deployments);
	}
	
	public boolean hasIntroducedBy() {
		return hasFilter(introducedBy);
	}
	
	public boolean hasTypes() {
		return ((types != null) && (types.length() > 0));
	}
	
	public Collection<String> getApplications(String serviceId) {
		return getServiceFilters(applications, serviceId);
	}
	
	public Collection<String> getDeployments(String serviceId) {
		return getServiceFilters(deployments, serviceId);
	}
	
	public Collection<String> getServers(String serviceId) {
		return getServiceFilters(servers, serviceId);
	}
	
	public Collection<String> getIntroducedBy(String serviceId) {
		
		if (introducedBy == null) {
			return Collections.emptySet();
		}
		
		return getServiceFilters(introducedBy, serviceId);
	}
	
	public Collection<String> getTypes() {
		
		if (types == null) {
			return Collections.emptySet();
		}
		
		return getServiceFilters(types, null);
	}
	
	public Collection<String> getTransactions(String serviceId) {
		
		if ((transactions == null) || (transactions.length() == 0)){
			return null;
		}
	
		return getServiceFilters(transactions, serviceId);
	}
	
	public EventFilter getEventFilter(String serviceId) {
		return EventFilter.of(getTypes(), getIntroducedBy(serviceId), getTransactions(serviceId));
	}
	
}
