package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.Map;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.category.Category;
import com.takipi.api.client.data.view.SummarizedView;
import com.takipi.api.client.data.view.ViewInfo;
import com.takipi.api.client.util.category.CategoryUtil;
import com.takipi.api.client.util.view.ViewUtil;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
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
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {

		if (!(input instanceof ViewsInput)) {
			throw new IllegalArgumentException("input");
		}
		
		ViewsInput viewsInput = (ViewsInput)input;
		
		if (viewsInput.category != null) {
			
			Category category = CategoryUtil.getServiceCategoryByName(apiClient, serviceId, viewsInput.category);
			
			if ((category == null) || (category.views == null)) {
				return;
			}
			
			for (ViewInfo view : category.views) {
				String viewName = getServiceValue(view.name, serviceId, serviceIds);	
				appender.append(viewName);
			}
		} else {
			
			Map<String, SummarizedView> serviceViews = ViewUtil.getServiceViewsByName(apiClient, serviceId);
			
			for (SummarizedView view : serviceViews.values()) {
				String viewName = getServiceValue(view.name, serviceId, serviceIds);	
				appender.append(viewName);
			}
		}		
	}
}
