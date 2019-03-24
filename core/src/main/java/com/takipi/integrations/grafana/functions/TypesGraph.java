
package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.settings.GeneralSettings;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.input.TypesGraphInput;

public class TypesGraph extends GraphFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TypesGraph(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TypesGraphInput.class;
		}

		@Override
		public String getName() {
			return "typesGraph";
		}
	}

	public TypesGraph(ApiClient apiClient) {
		super(apiClient);
	}

	private GraphInput getInput(GraphInput input, String type) {
		Gson gson = new Gson();
		String json = gson.toJson(input);
		GraphInput result = gson.fromJson(json, GraphInput.class);
		result.types = type;
		return result;
	}

	@Override
	protected String getSeriesName(BaseGraphInput input, String seriesName,String serviceId,
			Collection<String> serviceIds) {
		return getServiceValue(input.types, serviceId, serviceIds);
	}

	@Override
	protected boolean isAsync(Collection<String> serviceIds) {
		return true;
	}
	
	@Override
	protected Collection<Callable<Object>> getTasks(Collection<String> serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {

		TypesGraphInput graphInput = (TypesGraphInput) input;

		List<Callable<Object>> result = new ArrayList<Callable<Object>>();

		for (String serviceId : serviceIds) {

			String viewId = getViewId(serviceId, input.view);

			if (viewId == null) {
				continue;
			}

			Collection<String> types = input.getTypes(apiClient, serviceId);
			
			if (types != null) {
				for (String type : types) {
					result.add(new GraphAsyncTask(serviceId, viewId, input.view, getInput(graphInput , type), timeSpan,
							serviceIds, pointsWanted));
				}
			} else {
				
				Collection<String> defaultTypes = graphInput.getDefaultTypes();
				
				if (defaultTypes == null) {
					GeneralSettings generalSettings = getSettings(serviceId).general;
					
					if (generalSettings != null) {
						defaultTypes = generalSettings.getDefaultTypes();
					}
				}
				
				if (defaultTypes != null) {
					for (String type : defaultTypes) {
						result.add(new GraphAsyncTask(serviceId, viewId, input.view, getInput(graphInput , type), timeSpan,
								serviceIds, pointsWanted));
					}
				} 	
			}
			
			if (result.size() == 0) {
				result.add(new GraphAsyncTask(serviceId, viewId, input.view, 
						getInput(graphInput, GrafanaFunction.ALL),
						timeSpan, serviceIds, pointsWanted));

			}
		}

		return result;
	}
}
