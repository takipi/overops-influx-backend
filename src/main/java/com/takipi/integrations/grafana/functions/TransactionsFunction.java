package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.view.SummarizedView;
import com.takipi.common.api.result.event.EventResult;
import com.takipi.common.api.util.Pair;
import com.takipi.common.udf.util.ApiViewUtil;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class TransactionsFunction extends GrafanaFunction {
	
	private static final String KEY_VALUE = "application";
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return ViewInput.class;
		}
		
		@Override
		public String getName() {
			return "transactions";
		}
	}

	public TransactionsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	public  List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof ViewInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		ViewInput request = (ViewInput)functionInput;
		
		String[] services = getServiceIds(request);
		
		Series series = new Series();
		series.name = SERIES_NAME;
		series.columns = Arrays.asList(new String[] { KEY_COLUMN, VALUE_COLUMN });
		series.values = new ArrayList<List<Object>>();
		
		Pair<DateTime, DateTime> timespan = TimeUtils.getTimeFilter(request.timeFilter);	
		
		for (String serviceId : services) {
			SummarizedView view = ApiViewUtil.getServiceViewByName(apiClient, serviceId, request.view);
		
			if (view == null) {
				continue;
			}
			
			List<EventResult> events = ApiViewUtil.getEvents(apiClient, serviceId, view.id, timespan.getFirst(), timespan.getSecond());
			
			for (EventResult event : events) {
				
				String entryPoint =  getSimpleClassName(event.entry_point.class_name);
				series.values.add(Arrays.asList(new Object[] {KEY_VALUE, entryPoint}));
			}
		}
		
		return Collections.singletonList(series);
	}
}
