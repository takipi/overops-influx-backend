package com.takipi.integrations.grafana.input;

import java.util.Collection;
import java.util.List;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings;

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
		return getApplications(apiClient, serviceId, true);
	}
	
	public Collection<String> getApplications(ApiClient apiClient, String serviceId,
		boolean expandGroups) {
		
		List<String> apps = getServiceFilters(applications, serviceId, true);
		
		if (apps == null) {
			return null;
		}
			
		Collection<String> result;
		
		if ((apiClient != null) && (expandGroups)) {			
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
		
		return Objects.equal(applications, other.applications) 
				&& Objects.equal(deployments, other.deployments)
				&& Objects.equal(servers, other.servers);
	}
	
}
