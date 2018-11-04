package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Objects;
import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.util.ArrayUtil;

public class EnvironmentsFilterInput extends EnvironmentsInput {
	
	public String applications;
	public String servers;
	public String deployments;
	
	protected List<String> getServiceFilters(String value, String serviceId, boolean matchCase) {

		if (GrafanaFunction.VAR_ALL.equals(value)) {
			return Collections.emptyList();
		}

		String[] values = ArrayUtil.safeSplitArray(value, GrafanaFunction.GRAFANA_SEPERATOR, false);
		Set<String> result = new HashSet<String>();

		for (int i = 0; i < values.length; i++) {
			String service = values[i];
			String clean = service.replace("(", "").replace(")", "");

			if (!matchCase) {
				clean = clean.toLowerCase();
			}

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

		return new ArrayList<String>(result);
	}

	protected boolean hasFilter(String value) {
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
	
	public List<String> getApplications(String serviceId) {
		return getServiceFilters(applications, serviceId, true);
	}

	public List<String> getDeployments(String serviceId) {
		return getServiceFilters(deployments, serviceId, true);
	}

	public List<String> getServers(String serviceId) {
		return getServiceFilters(servers, serviceId, true);
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if (!super.equals(obj)) {
			return false;
		}
		
		if (!(obj instanceof EnvironmentsFilterInput)) {
			return false;
		}
		
		EnvironmentsFilterInput other = (EnvironmentsFilterInput)obj;
		
		return Objects.equal(other.applications, other.applications) 
				&& Objects.equal(other.deployments, other.deployments)
				&& Objects.equal(other.servers, other.servers);
	}
	
}
