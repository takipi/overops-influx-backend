package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.common.udf.util.ApiFilterUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.output.Series;

public class ApplicationsFunction extends GrafanaFunction {
	
	private static final String KEY_VALUE = "application";
	
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
	public QueryResult process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof EnvironmentsInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		EnvironmentsInput request = (EnvironmentsInput)functionInput;
		
		String[] services = getServiceIds(request);
		
		Series series = new Series();
		series.name = SERIES_NAME;
		series.columns = Arrays.asList(new String[] { KEY_COLUMN, VALUE_COLUMN });
		series.values = new ArrayList<List<Object>>();
		
		for (String serviceId : services) {
			List<String> serviceApps = ApiFilterUtil.getApplications(apiClient, serviceId);
			
			for (String app : serviceApps) {
				
				String appName;
				
				if (services.length == 1) {
					appName = app;
				} else {
					appName = app + SERVICE_SEPERATOR + serviceId;
				}
			
				series.values.add(Arrays.asList(new Object[] {KEY_VALUE, appName}));
			}
		}
		
		return createQueryResults(Collections.singletonList(series));
	}
}
