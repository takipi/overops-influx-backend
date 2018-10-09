package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.category.Category;
import com.takipi.common.api.data.view.ViewInfo;
import com.takipi.common.api.request.category.CategoriesRequest;
import com.takipi.common.api.result.category.CategoriesResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.common.api.util.Pair;
import com.takipi.integrations.grafana.input.CategoryInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.output.QueryResult;
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
	protected Map<String, String> getViews(String serviceId, GraphInput input) {
		
		CategoriesRequest request = CategoriesRequest.newBuilder().setServiceId(serviceId).build();
		Response<CategoriesResult> response = apiClient.get(request);
		
		if (response.isBadResponse()) {
			throw new IllegalStateException("Could not list categories in" + serviceId + ". Code " + response.responseCode);
		}
		
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
	
	protected List<Series> processSeries(List<Pair<Series, Long>> series, GraphInput request) {
		
		CategoryInput categoryInput = (CategoryInput)request;
		List<Series> output = super.processSeries(series, request);
		
		if (categoryInput.viewLimit == 0) {
			return output;
		}
		
		series.sort(new Comparator<Pair<Series, Long>>() {

			@Override
			public int compare(Pair<Series, Long> o1, Pair<Series, Long> o2) {
				return (int)(o1.getSecond().longValue() - o2.getSecond().longValue());
			}
		});
		
		List<Series> result = new ArrayList<Series>();
		
		for (int i = 0; i < Math.min(categoryInput.viewLimit, series.size()); i++) {
			result.add(series.get(i).getFirst());
		}
		
		return result;
	}
	
	@Override
	public QueryResult process(FunctionInput functionIput) {
		if (!(functionIput instanceof CategoryInput)) {
			throw new IllegalArgumentException("functionIput");
		}
		
		return super.process(functionIput);
	}
	
}
