package com.takipi.integrations.grafana.input;

import java.util.Collection;
import java.util.Collections;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.settings.GrafanaSettings;

public class RegressionsInput extends EventsInput {
	
	
	public Collection<String> getCriticalExceptionTypes(ApiClient apiClient, String serviceId) {

		if (types == null) {
			return Collections.emptySet();
		}

		String criticalExceptionTypes = GrafanaSettings.
			getServiceSettings(apiClient, serviceId).regressionSettings.criticalExceptionTypes;
		
		return getServiceFilters(criticalExceptionTypes, null, false);
	}
}
