package com.takipi.integrations.grafana.input;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.settings.GroupSettings;
import com.takipi.api.client.util.settings.GroupSettings.Group;
import com.takipi.api.client.util.settings.GroupSettings.GroupFilter;
import com.takipi.api.client.util.settings.ServiceSettingsData;
import com.takipi.common.util.CollectionUtil;
import com.takipi.integrations.grafana.util.ApiCache;

/**
 * Input for functions that use a filter to request data from a specific combination
 * of applications, deployments and / or server groups.
 */
public abstract class EnvironmentsFilterInput extends BaseEnvironmentsInput {
	
	public static final String APP_LABEL_PREFIX = "--";
	
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
	
	public Collection<String> getApplications(ApiClient apiClient,
		ServiceSettingsData settingsData, String serviceId,
		boolean expandGroups, boolean includeLabelApps) {
		
		return getApplications(apiClient, settingsData, serviceId, applications,
				expandGroups, includeLabelApps);
	}
	
	public static Collection<String> getApplications(ApiClient apiClient, 
		ServiceSettingsData settingsData, String serviceId,
		String applications, boolean expandGroups, boolean includeLabelApps) {
		
		Collection<String> apps = getServiceFilters(applications, serviceId, true);
		
		if (apps == null) {
			return null;
		}
			
		Set<String> result = new HashSet<String>();
				
		if ((settingsData != null) && (settingsData.applications != null) && (expandGroups)) {			
				
			Collection<String> serviceApps = null;

			for (String app : apps) {
				
				if (isLabelApp(app)) {
					if (includeLabelApps) {
						result.addAll(apps);
					}
				} else if (GroupSettings.isGroup(app)) {
					
					Group group = settingsData.applications.getGroup(app);
					
					if (group != null) {
						
						GroupFilter filter = group.getFilter();
						
						if (!CollectionUtil.safeIsEmpty(filter.patterns)) {
							
							if (serviceApps == null) {
								serviceApps  = ApiCache.getApplicationNames(apiClient, serviceId, false, null);
							}
							
							for (String serviceApp : serviceApps) {
								 
								 if (!filter.filter(serviceApp)) {
									 result.add(serviceApp);
								 }
							 }	
						}
						
						if (!CollectionUtil.safeIsEmpty(filter.values)) {
							result.addAll(filter.values);
						}		
					} else {
						result.add(GroupSettings.fromGroupName(app));
					}
				} else {
					result.add(app);
				}				
			} 			
		} else {
			if (includeLabelApps) {
				result.addAll(apps);
			} else {
				for (String app : apps) {
					if (!isLabelApp(app)) {
						result.add(app);
					}
				}
			}
		}
		
		return result;
	}
	
	public static String toAppLabel(String app) {
		return APP_LABEL_PREFIX + app;
	}
	
	public static boolean isLabelApp(String app) {
		return app.startsWith(APP_LABEL_PREFIX);
	}
	
	public static String getLabelAppName(String value) {
		
		if ((value == null) || (!isLabelApp(value))) {
			return value;
		}

		return value.substring(APP_LABEL_PREFIX.length());
	}

	public Collection<String> getDeployments(String serviceId) {
		return getDeployments(serviceId, null, false);
	}
	
	public Collection<String> getDeployments(String serviceId, ApiClient apiClient) {
		return getDeployments(serviceId, apiClient, true);
	}
	
	private Collection<String> getDeployments(String serviceId, ApiClient apiClient, 
			boolean expandGroups) {
		
		Collection<String> serviceValues = getServiceFilters(deployments, serviceId, true);
		
		if (serviceValues == null) {
			return null;
		}
		
		Set<String> result = new HashSet<String>();				
		
		if (expandGroups) {
			
			GroupFilter filter = GroupFilter.from(serviceValues);
			
			if (!CollectionUtil.safeIsEmpty(filter.patterns)) {
				
				Collection<String> deps = ApiCache.getDeploymentNames(apiClient, serviceId, false, null);
				
				for (String dep : deps) {
					 
					 if (!filter.filter(dep)) {
						 result.add(dep);
					 }
				 }
			}
			
			if (!CollectionUtil.safeIsEmpty(filter.values)) {
				result.addAll(filter.values);
			}
		} else {
			result.addAll(serviceValues);
		}
	
		return result;
	}

	public Collection<String> getServers(String serviceId) {
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
