package com.takipi.integrations.grafana.functions;

import java.util.Map;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.category.Category;
import com.takipi.common.api.data.view.SummarizedView;
import com.takipi.common.api.data.view.ViewInfo;
import com.takipi.common.udf.util.ApiCategoryUtil;
import com.takipi.common.udf.util.ApiViewUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.ViewsInput;

public class ViewsFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ViewsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return ViewsInput.class;
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

		if (!(input instanceof ViewsInput)) {
			throw new IllegalArgumentException("input");
		}
		
		ViewsInput viewsInput = (ViewsInput)input;
		
		if (viewsInput.category != null) {
			
			Category category = ApiCategoryUtil.getServiceCategoryByName(apiClient, serviceId, viewsInput.category);
			
			if ((category == null) || (category.views == null)) {
				return;
			}
			
			for (ViewInfo view : category.views) {
				String viewName = getServiceValue(view.name, serviceId, serviceIds);	
				appender.append(viewName);
			}
		} else {
			
			Map<String, SummarizedView> serviceViews = ApiViewUtil.getServiceViewsByName(apiClient, serviceId);
			
			for (SummarizedView view : serviceViews.values()) {
				String viewName = getServiceValue(view.name, serviceId, serviceIds);	
				appender.append(viewName);
			}
		}		
	}
}
