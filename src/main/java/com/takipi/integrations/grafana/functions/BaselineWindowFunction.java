package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.Date;

import org.joda.time.DateTime;
import org.ocpsoft.prettytime.PrettyTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.BaselineWindowInput;
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
			return BaselineWindowInput.class;
		}

		@Override
		public String getName() {
			return "baselineWindow";
		}
	}

	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender)
	{		
		BaselineWindowInput bwInput = (BaselineWindowInput)input;
		
		String viewId = getViewId(serviceId, bwInput.view);
		
		if (viewId == null)
		{
			return;
		}
		
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(bwInput.timeFilter);
		
		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		Pair<RegressionInput, RegressionWindow> inputPair = regressionFunction.getRegressionInput(serviceId, viewId, bwInput, timespan);
		
		int time;
		
		switch (bwInput.getWindowType()) {
			
			case Baseline: {
				time = inputPair.getFirst().baselineTimespan;
				break;
			}
			
			case Active: {
				time = inputPair.getSecond().activeTimespan;
				break;
			}
			
			case Combined: {
				time = inputPair.getFirst().baselineTimespan + inputPair.getSecond().activeTimespan;
				break;
			}
			
			default: 
				throw new IllegalStateException(bwInput.getWindowType().toString());	
		}
			
		String value;
		
		if (bwInput.prettyFormat) {
			Date duration = DateTime.now().minusMinutes(time).toDate();
			value = new PrettyTime().formatDuration(duration);
		} else {
			value = time + TimeUtil.MINUTE_POSTFIX;
		}
				
		appender.append(value);
	}
}
