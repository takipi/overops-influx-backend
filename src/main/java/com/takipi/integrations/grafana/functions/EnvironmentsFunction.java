package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.service.SummarizedService;
import com.takipi.common.udf.util.ApiFilterUtil;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.output.Series;

public class EnvironmentsFunction extends GrafanaFunction {
	
	private static final String KEY_VALUE = "environment";
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new EnvironmentsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return FunctionInput.class;
		}
		
		@Override
		public String getName() {
			return "environments";
		}
	}

	public EnvironmentsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	public QueryResult process(FunctionInput functionInput) {
		
		List<SummarizedService> services =  ApiFilterUtil.getEnvironments(apiClient);
		
		Series series = new Series();

		series.name = SERIES_NAME;
		series.columns = Arrays.asList(new String[] { KEY_COLUMN, VALUE_COLUMN });
		series.values = new ArrayList<List<Object>>(services.size());
		
		for (SummarizedService service : services) {
			series.values.add(Arrays.asList(new Object[] {KEY_VALUE, service.name + SERVICE_SEPERATOR + service.id}));
		}
		
		return createQueryResults(Collections.singletonList(series));
	}
}
