package com.takipi.integrations.grafana.functions;

import java.util.Map;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.view.SummarizedView;
import com.takipi.common.udf.util.ApiViewUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;

public class ViewsFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ViewsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EnvironmentsInput.class;
		}

		@Override
		public String getName() {
			return "views";
		}
	}

	public ViewsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, String[] serviceIds, String serviceId,
			VariableAppender appender) {

		Map<String, SummarizedView> serviceViews = ApiViewUtil.getServiceViewsByName(apiClient, serviceId);

		for (SummarizedView view : serviceViews.values()) {

			String viewName = getServiceValue(view.name, serviceId, serviceIds);	
			appender.append(viewName);
		}
	}
}
