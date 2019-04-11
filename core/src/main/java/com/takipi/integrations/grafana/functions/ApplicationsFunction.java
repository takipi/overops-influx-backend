package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.infra.Categories.CategoryType;
import com.takipi.api.client.util.infra.InfraUtil;
import com.takipi.api.client.util.settings.GroupSettings;
import com.takipi.api.client.util.settings.GroupSettings.Group;
import com.takipi.integrations.grafana.input.ApplicationsInput;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.EnvironmentsFilterInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.util.ApiCache;

public class ApplicationsFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ApplicationsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return ApplicationsInput.class;
		}

		@Override
		public String getName() {
			return "applications";
		}
	}

	public ApplicationsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {
	
		GroupSettings appGroupSettings = getSettingsData(serviceId).applications;
		
		if (appGroupSettings != null) {
			
			for (Group appGroup : appGroupSettings.getGroups()) {	
				String appGroupName = getServiceValue(appGroup.toGroupName(), serviceId, serviceIds);
				appender.append(appGroupName);
			}
		}
				
		Collection<String> activeApps = ApiCache.getApplicationNames(apiClient, serviceId, true);
		Collection<String> nonActiveApps = ApiCache.getApplicationNames(apiClient, serviceId, false);
		Collection<String> labelsApps = getLabelApps(serviceId);

		
		Collection<String> apps = addApps(input, serviceId, serviceIds, activeApps, appender, null);		
		addApps(input, serviceId, serviceIds, nonActiveApps, appender, apps);
		addApps(input, serviceId, serviceIds, labelsApps, appender, apps);

	}
	
	private Collection<String> getLabelApps(String serviceId) {
		
		Collection<String> labels = ApiCache.getLabelNames(apiClient, serviceId);
		Set<String> result = new HashSet<String>();
		
		for (String label : labels) {
			
			String app = InfraUtil.getTierNameFromLabel(label, CategoryType.App);
			
			if (app == null) {
				continue;		
			}
			
			result.add(EnvironmentsFilterInput.toAppLabel(app));
		}
		
		return result;
	}
	
	@Override
	protected void sortValues(FunctionInput input, List<List<Object>> series) {
		//do nothing
	}

	private Collection<String> addApps(BaseEnvironmentsInput input, String serviceId, Collection<String> serviceIds, Collection<String> apps,
		VariableAppender appender, Collection<String> existing) {
		
		Collection<String> result;
		
		if (input.sorted) {
			result = new TreeSet<String>(apps); 
		} else {
			result = apps;
		}
				
		for (String app : result) {

			if ((existing != null) && (existing.contains(app))) {
				continue;
			}
			
			String serviceApp = getServiceValue(app, serviceId, serviceIds);
			appender.append(serviceApp);
		}
		
		return result;
	}
	
	
}
