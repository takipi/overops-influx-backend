package com.takipi.integrations.grafana.functions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionStringUtil;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionsNameInput;
import com.takipi.integrations.grafana.output.Series;

public class RegressionNameFunction extends GrafanaFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new RegressionNameFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return RegressionsNameInput.class;
		}
		
		@Override
		public String getName() {
			return "regressionName";
		}
	}
	
	public RegressionNameFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	private String getRegressionName(RegressionsNameInput input, String serviceId) {
				
		RegressionInput regressionInput = new RegressionInput();
		
		regressionInput.serviceId = serviceId;
		regressionInput.viewId = getViewId(serviceId, input.view);
		
		if (regressionInput.viewId == null) {
			return null;
		}

		regressionInput.activeTimespan = input.activeTimespan;
		regressionInput.baselineTimespan = input.baselineTimespan;

		regressionInput.applictations = input.getApplications(serviceId);
		regressionInput.servers = input.getServers(serviceId);
		regressionInput.deployments = input.getDeployments(serviceId);
		
		return RegressionStringUtil.getRegressionName(apiClient, regressionInput);
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof RegressionsNameInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		RegressionsNameInput input = (RegressionsNameInput)functionInput;
		
		String[] serviceIds = getServiceIds(input);
		
		if (serviceIds.length == 0) {
			return null;
		}
		
		String name = getRegressionName(input, serviceIds[0]);
		
		if (name == null) {
			return null;
		}
		
		Series series = new Series();
		
		series.name = SERIES_NAME;
		series.columns = Arrays.asList(new String[] { KEY_COLUMN, VALUE_COLUMN });
		series.values = Collections.singletonList(Arrays.asList(new Object[] {KEY_COLUMN, name}));
		
		return Collections.singletonList(series);	
	}

}
