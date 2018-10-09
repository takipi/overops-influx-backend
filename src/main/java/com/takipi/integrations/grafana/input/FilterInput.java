package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.utils.ArrayUtils;

public class FilterInput extends EnvironmentsInput {
	
	public String timeFilter;
	public String applications;
	public String servers;
	public String deployments;
	
	private Collection<String> getServiceFilters(String value, String serviceId) {
		
		if (GrafanaFunction.VAR_ALL.equals(value)) {
			return Collections.emptySet();
		}
		
		String[] values = ArrayUtils.safeSplitArray(value, GrafanaFunction.GRAFANA_SEPERATOR, false);
		List<String> result = new ArrayList<String>();
		
		for (int i= 0; i < values.length; i++) {
			String service = values[i];
			String clean = service.replace("(", "").replace(")", "");
			
			String[] parts = clean.split(GrafanaFunction.SERVICE_SEPERATOR);
				
			if (parts.length > 1) {
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
		return (value != null) && (value.length() == 0) && (!value.equals(GrafanaFunction.VAR_ALL));

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
	
	public Collection<String> getApplications(String serviceId) {
		return getServiceFilters(applications, serviceId);
	}
	
	public Collection<String> getDeployments(String serviceId) {
		return getServiceFilters(deployments, serviceId);
	}
	
	public Collection<String> getServers(String serviceId) {
		return getServiceFilters(servers, serviceId);
	}
}