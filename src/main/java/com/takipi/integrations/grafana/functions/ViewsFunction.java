package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.view.SummarizedView;
import com.takipi.common.udf.util.ApiViewUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;

public class ViewsFunction extends GrafanaFunction {
	
	private static final String KEY_VALUE = "view";
	
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
			Map<String, SummarizedView> serviceViews = ApiViewUtil.getServiceViewsByName(apiClient, serviceId);
			
			for (SummarizedView view : serviceViews.values()) {
				
				if (view.name != null) {
					series.values.add(Arrays.asList(new Object[] {KEY_VALUE, view.name}));
				}
			}
		}
		
		series.values.sort(new Comparator<Object>() {

			@SuppressWarnings("unchecked")
			@Override
			public int compare(Object o1, Object o2) {
				String a = ((List<Object>)o1).get(1).toString().toLowerCase();
				String b = ((List<Object>)o2).get(1).toString().toLowerCase();
				
				return a.compareTo(b);
			}
		});
		
		return Collections.singletonList(series);
	}
}
