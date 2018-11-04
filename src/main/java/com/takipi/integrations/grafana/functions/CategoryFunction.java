package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.category.Category;
import com.takipi.api.client.data.view.ViewInfo;
import com.takipi.api.client.request.category.CategoriesRequest;
import com.takipi.api.client.result.category.CategoriesResult;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.CategoryInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;

public class CategoryFunction extends GraphFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new CategoryFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return CategoryInput.class;
		}
		
		@Override
		public String getName() {
			return "category";
		}
	}
	
	public CategoryFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	@Override
	protected Map<String, String> getViews(String serviceId, BaseGraphInput input) {
		
		CategoriesRequest request = CategoriesRequest.newBuilder().setServiceId(serviceId).build();
		Response<CategoriesResult> response = apiClient.get(request);
		
		validateResponse(response);
		
		if (response.data.categories == null) {
			return Collections.emptyMap();
		}
		
		CategoryInput categoryInput = (CategoryInput)input;
				
		for (Category category : response.data.categories) {
			if (categoryInput.category.equals(category.name)) {
				
				if (category.views == null) {
					continue;
				}
				
				Map<String, String> result = new HashMap<String, String>();
				
				for (ViewInfo viewInfo : category.views) {
					result.put(viewInfo.id, viewInfo.name);
				}
				
				return result;
			}
		}
		
		return Collections.emptyMap();
	}
	
	@Override
	protected boolean isAsync(String[] serviceIds) {
		return true;
	}
	
	@Override
	protected List<Series> processSeries(List<GraphSeries> series, BaseGraphInput input) {
		List<Series> output = super.processSeries(series, input);
		
		CategoryInput categoryInput = (CategoryInput)input;
		
		if (categoryInput.limit == 0) {
			return output;
		}
		
		sortSeriesByVolume(series);
		
		List<Series> result = new ArrayList<Series>();
		
		for (int i = 0; i < Math.min(categoryInput.limit, series.size()); i++) {
			GraphSeries graphSeries = series.get(i);
			
			if (graphSeries.volume > 0) {
				result.add(graphSeries.series);
			}
		}
		
		sortByName(result);
		
		return result;
	}
	
	private void sortSeriesByVolume(List<GraphSeries> series) {
		series.sort(new Comparator<GraphSeries>() {

			@Override
			public int compare(GraphSeries o1, GraphSeries o2) {
				return (int)(o2.volume - o1.volume);
			}
		});
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		if (!(functionInput instanceof CategoryInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
	}
	
}
