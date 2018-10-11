package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.common.udf.util.ApiFilterUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;

public class ServersFunction extends GrafanaFunction {
	
	private static final String KEY_VALUE = "server";
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ServersFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EnvironmentsInput.class;
		}
		
		@Override
		public String getName() {
			return "servers";
		}
	}

	public ServersFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	public  List<Series> process(FunctionInput functionInput) {
		
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
			List<String> serviceServers = ApiFilterUtil.getSevers(apiClient, serviceId);
			
			for (String server : serviceServers) {
				
				String serverName;
				
				if (services.length == 1) {
					serverName = server;
				} else {
					serverName = server + SERVICE_SEPERATOR + serviceId;
				}
			
				series.values.add(Arrays.asList(new Object[] {KEY_VALUE, serverName}));
			}
		}
		
		return Collections.singletonList(series);
	}
}
