package com.takipi.integrations.grafana.functions;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TimeFilterInput;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TimeFilterFunction extends VariableFunction {
	
	public TimeFilterFunction(ApiClient apiClient) {
		super(apiClient);
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TimeFilterFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TimeFilterInput.class;
		}

		@Override
		public String getName() {
			return "timeFilter";
		}
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {
		
		TimeFilterInput tfInput = (TimeFilterInput)input;
		
		String value;
		
		if (tfInput.limit != null) {
			
			Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(tfInput.timeFilter);
			String timeRange = TimeUtil.getTimeRange(timespan);
			
			if (timeRange != null) {
				
				int rangeMin = TimeUtil.parseInterval(timeRange);
				int limitMin = TimeUtil.parseInterval(tfInput.limit);
				
				int min = Math.min(rangeMin, limitMin);
				
				if (tfInput.rangeOnly) {
					value = tfInput.rangePrefix + TimeUtil.getTimeRange(min);
				} else {
					value = TimeUtil.getLastWindowMinTimeFilter(min);	
				}
			} else {
				value = tfInput.timeFilter;	
			}
		} else {
			value = tfInput.timeFilter;
		}
		
		appender.append(value);
	}
}
