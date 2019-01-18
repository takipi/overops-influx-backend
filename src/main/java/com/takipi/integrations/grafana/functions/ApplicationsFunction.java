package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.integrations.grafana.input.ApplicationsInput;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings;
import com.takipi.integrations.grafana.settings.GroupSettings.Group;

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
	
		GroupSettings appGroupSettings = GrafanaSettings.getData(apiClient, serviceId).applications;
		
		if (appGroupSettings != null) {
			
			for (Group appGroup : appGroupSettings.getGroups()) {	
				String appGroupName = getServiceValue(appGroup.toGroupName(), serviceId, serviceIds);
				appender.append(appGroupName);
			}
		}
				
		Collection<String> liveApps = addApps(input, serviceId, serviceIds,
			ClientUtil.getApplications(apiClient, serviceId, true), appender, null);
		
		addApps(input, serviceId, serviceIds,
				ClientUtil.getApplications(apiClient, serviceId, false), appender, liveApps);
	}
	
	@Override
	protected void sortValues(List<List<Object>> series)
	{
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
