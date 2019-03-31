package com.takipi.integrations.grafana.functions;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.MinTimeFilterAlertInput;
import com.takipi.integrations.grafana.util.TimeUtil;

public class MinTimeFilterAlertFunction extends VariableFunction {
	
	public MinTimeFilterAlertFunction(ApiClient apiClient) {
		super(apiClient);
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new MinTimeFilterAlertFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return MinTimeFilterAlertInput.class;
		}

		@Override
		public String getName() {
			return "minTimeFilterAlert";
		}
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {
		
		MinTimeFilterAlertInput mtfInput = (MinTimeFilterAlertInput)input;
		
		if ((mtfInput.minRange == null) || (mtfInput.text == null)) {
			appender.append("");
			return;
		}
		
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(mtfInput.timeFilter);
		int interval = TimeUtil.parseInterval(mtfInput.minRange);
		
		if (timespan.getSecond().getMillis() - timespan.getFirst().getMillis() < 
			TimeUnit.MINUTES.toMillis(interval)) {
			appender.append(mtfInput.text);
		} else {
			appender.append("");

		}
	}
}