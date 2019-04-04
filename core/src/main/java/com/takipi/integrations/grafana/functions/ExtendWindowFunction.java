package com.takipi.integrations.grafana.functions;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.ExtendWindowInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.util.TimeUtil;

public class ExtendWindowFunction extends VariableFunction {	
	
	public ExtendWindowFunction(ApiClient apiClient) {
		super(apiClient);
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ExtendWindowFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return ExtendWindowInput.class;
		}

		@Override
		public String getName() {
			return "extendWindow";
		}
	}

	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {

		ExtendWindowInput ewInput = (ExtendWindowInput)input;
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(ewInput.timeFilter);
		long delta = TimeUtil.getDateTimeDeltaMill(timespan);
		
		long rangeMilli;
		
		if (ewInput.range != null) {
			int rangeValue = TimeUtil.parseInterval(ewInput.range);
			rangeMilli = delta + TimeUnit.MINUTES.toMillis(rangeValue);
		} else {
			rangeMilli = delta;
		}
		
		String value = TimeUtil.getTimeInterval(rangeMilli);					
		appender.append(value);
	}
}
