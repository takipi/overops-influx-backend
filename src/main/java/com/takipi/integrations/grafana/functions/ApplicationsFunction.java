package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.settings.ApplicationGroupSettings;
import com.takipi.integrations.grafana.settings.ApplicationGroupSettings.AppGroup;
import com.takipi.integrations.grafana.settings.GrafanaSettings;

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
	
		ApplicationGroupSettings appGroupSettings = GrafanaSettings.getServiceSettings(apiClient, serviceId).applicationGroups;
		
		if ((appGroupSettings != null) && (appGroupSettings.groups != null)) {
			
			for (AppGroup appGroup : appGroupSettings.groups) {	
				String appGroupName = getServiceValue(EventFilter.CATEGORY_PREFIX + appGroup.name, serviceId, serviceIds);
				appender.append(appGroupName);
			}
		}
		
		List<String> serviceApps = ClientUtil.getApplications(apiClient, serviceId);

		for (String app : serviceApps) {

			String serviceApp = getServiceValue(app, serviceId, serviceIds);
			appender.append(serviceApp);
		}
	}
}
