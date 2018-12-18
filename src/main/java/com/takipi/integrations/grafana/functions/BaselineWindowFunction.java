package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.EventFilterInput;
import com.takipi.integrations.grafana.util.TimeUtil;

public class BaselineWindowFunction extends EnvironmentVariableFunction
{	
	public BaselineWindowFunction(ApiClient apiClient)
	{
		super(apiClient);
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new BaselineWindowFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EventFilterInput.class;
		}

		@Override
		public String getName() {
			return "baselineWindow";
		}
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender)
	{		
		EventFilterInput viewInput = (EventFilterInput)input;
		
		String viewId = getViewId(serviceId, viewInput.view);
		
		if (viewId == null)
		{
			return;
		}
		
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(viewInput.timeFilter);
		
		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		Pair<RegressionInput, RegressionWindow> inputPair = regressionFunction.getRegressionInput(serviceId, viewId, viewInput, timespan);
		
		long time = inputPair.getFirst().baselineTimespan + inputPair.getSecond().activeTimespan;
		
		String value = TimeUtil.getTimeInterval(TimeUnit.MINUTES.toMillis(time));
		
		appender.append(value);
	}
}
