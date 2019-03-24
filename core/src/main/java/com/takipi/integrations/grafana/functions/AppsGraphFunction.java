package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.settings.GroupSettings;
import com.takipi.api.client.util.regression.settings.ServiceSettingsData;
import com.takipi.api.client.util.regression.settings.GroupSettings.Group;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.EnvironmentsFilterInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GraphLimitInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.ApiCache;

public class AppsGraphFunction extends BaseServiceCompositeFunction
{
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new AppsGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return GraphLimitInput.class;
		}

		@Override
		public String getName() {
			return "appsGraph";
		}
	}
	
	protected class AppGraphFunction extends GraphFunction {

		public AppGraphFunction(ApiClient apiClient) {
			super(apiClient);
		}
		
		public AppGraphFunction(ApiClient apiClient, Map<String, ServiceSettingsData> settingsMaps) {
			super(apiClient, settingsMaps);
		}
		
		@Override
		protected String getSeriesName(BaseGraphInput input, String seriesName, 
				String serviceId, Collection<String> serviceIds) {
			String result = getServiceValue(input.applications, serviceId, serviceIds);
			return result;		
		}
		
		@Override
		protected List<Series> processSeries(List<GraphSeries> series, BaseGraphInput input) {
			
			List<GraphSeries> nonEmptySeries = new ArrayList<GraphSeries>(series.size());
			
			for (GraphSeries graphSeries : series) {
				if (graphSeries.volume > 0) {
					nonEmptySeries.add(graphSeries);
				}
			}
			
			List<Series> result = super.processSeries(nonEmptySeries, input);
								
			return result;
		}
	}
	
	public AppsGraphFunction(ApiClient apiClient)
	{
		super(apiClient);
	}
		
	public Collection<String> getApplications(String serviceId, 
		EnvironmentsFilterInput input, int limit) {

		List<String> result;
		Collection<String> selectedApps = input.getApplications(apiClient, getSettings(serviceId), serviceId, false);
		
		if (!CollectionUtil.safeIsEmpty(selectedApps)) {
			
			result = new ArrayList<String>(selectedApps);	
			return result;
		}
		
		List<String> keyApps = new ArrayList<String>();
		
		GroupSettings appGroups = getSettings(serviceId).applications;
		
		if (appGroups != null) {
			
			for (Group group : appGroups.getGroups()) {
				keyApps.add(group.toGroupName());
			}
		}
			
		if (keyApps.size() > limit) {
			return keyApps.subList(0, limit);
		}
		
		List<String> activeApps = new ArrayList<String>(ApiCache.getApplicationNames(apiClient, serviceId, true));  
		
		sortApplicationsByProcess(serviceId, activeApps,
			input.getServers(serviceId), input.getDeployments(serviceId, apiClient));	
		
		result = new ArrayList<String>(keyApps.size() + activeApps.size());
		result.addAll(keyApps);

		if (limit > 0) {
			
			for (String activeApp : activeApps) {
				
				if (!result.contains(activeApp)) {
					result.add(activeApp);
				}
				
				if (result.size() >= limit) {
					break;
				}
			}
		} else {
			result.addAll(activeApps);
		}	

		result.sort(null);
		
		return result;
	}
	
	@Override
	protected Collection<Pair<GrafanaFunction, FunctionInput>> getServiceFunctions(String serviceId,
			EnvironmentsFilterInput functionInput)
	{
		GraphLimitInput input = (GraphLimitInput)functionInput;
		List<Pair<GrafanaFunction, FunctionInput>> result = new ArrayList<Pair<GrafanaFunction,FunctionInput>>();
		
		Collection<String> apps = getApplications(serviceId, functionInput, input.limit);
		
		Gson gson = new Gson();
		String json = gson.toJson(functionInput);
		
		for (String app : apps) {
			EnvironmentsFilterInput appInput = gson.fromJson(json, functionInput.getClass());
			appInput.applications = app;
			result.add(Pair.of(new AppGraphFunction(apiClient, settingsMaps), appInput));
		}
		
		return result;
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput)
	{
		if (!(functionInput instanceof GraphLimitInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
	}
}
