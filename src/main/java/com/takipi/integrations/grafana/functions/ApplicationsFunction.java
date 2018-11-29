package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
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
			return EnvironmentsInput.class;
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
	protected void populateServiceValues(EnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {
	
		GroupSettings appGroupSettings = GrafanaSettings.getData(apiClient, serviceId).applications;
		
		if (appGroupSettings != null) {
			
			for (Group appGroup : appGroupSettings.getGroups()) {	
				String appGroupName = getServiceValue(appGroup.toGroupName(), serviceId, serviceIds);
				appender.append(appGroupName);
			}
		}
		
		List<String> serviceApps;
		
		try {
			serviceApps	= ClientUtil.getApplications(apiClient, serviceId);	
		} catch (Exception e) {
			System.err.println(e);
			serviceApps = Collections.emptyList();
		}
		
		for (String app : serviceApps) {

			String serviceApp = getServiceValue(app, serviceId, serviceIds);
			appender.append(serviceApp);
		}
	}
}
