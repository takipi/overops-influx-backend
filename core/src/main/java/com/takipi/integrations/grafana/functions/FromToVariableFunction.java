package com.takipi.integrations.grafana.functions;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.FromToInput;
import com.takipi.integrations.grafana.input.FromToInput.FromToType;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.util.TimeUtil;

public class FromToVariableFunction extends VariableFunction {
	
	public FromToVariableFunction(ApiClient apiClient) {
		super(apiClient);
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new FromToVariableFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return FromToInput.class;
		}

		@Override
		public String getName() {
			return "fromTo";
		}
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {
		
		FromToInput ftInput = (FromToInput)input;
		FromToType fromToType = ftInput.getFromToType();
		
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(ftInput.timeFilter);
				
		String value;
		
		String timeRange = TimeUtil.getTimeRange(timespan);
		
		if (timeRange != null) {
			
			if (fromToType == FromToType.To) {
				value = "now";
			} else {
			
				value = "now-" + timeRange;	
			}	
		} else {
			if (fromToType == FromToType.To) {
				value = String.valueOf(timespan.getSecond().getMillis());
			} else {
				value = String.valueOf(timespan.getFirst().getMillis());
			}	
		}
		
		appender.append(value);
	}
}
