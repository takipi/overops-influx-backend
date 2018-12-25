package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings;
import com.takipi.integrations.grafana.util.ArrayUtil;

/**
 * Input for functions that use a filter to request data from a specific combination
 * of applications, deployments and / or server groups.
 */
public abstract class EnvironmentsFilterInput extends BaseEnvironmentsInput {
	
	/**
	 * A comma delimited array of application names to use as a filter. Specify null, "", "all" or "*" to skip
	 */
	public String applications;
	
	/**
	 * A comma delimited array of server names to use as a filter. Specify null, "", "all" or "*" to skip
	 */
	
	public String servers;
	
	/**
	 * A comma delimited array of deployment names to use as a filter. Specify null, "", "all" or "*" to skip
	 */
	public String deployments;
	
	protected List<String> getServiceFilters(String value, String serviceId, boolean matchCase) {

		if (!hasFilter(value)) {
			return Collections.emptyList();
		}

		String[] values = ArrayUtil.safeSplitArray(value, GrafanaFunction.GRAFANA_SEPERATOR, false);
		Set<String> result = new HashSet<String>();

		for (int i = 0; i < values.length; i++) {
			String service = values[i];
			String clean = service.replace("(", "").replace(")", "");
			
			if (clean.length() == 0) {
				continue;
			}

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

	public static boolean hasFilter(String value) {
		return (value != null) && (value.length() != 0) && (!GrafanaFunction.VAR_ALL.contains(value));
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
	
	public Collection<String> getApplications(ApiClient apiClient, String serviceId) {
		
		List<String> apps = getServiceFilters(applications, serviceId, true);
		
		if (apps == null) {
			return null;
		}
			
		Collection<String> result;
		
		if (apiClient != null) {			
			GroupSettings groupSettings = GrafanaSettings.getData(apiClient, serviceId).applications;
			
			if (groupSettings != null) {
				result = groupSettings.expandList(apps);
			} else {
				result = apps;	
			}
		} else {
			result = apps;
		}
		
		return result;
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
