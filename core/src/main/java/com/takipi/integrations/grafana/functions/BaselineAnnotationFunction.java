package com.takipi.integrations.grafana.functions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.BaselineAnnotationInput;
import com.takipi.integrations.grafana.output.Series;

public class BaselineAnnotationFunction extends BaseGraphFunction {
		
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new BaselineAnnotationFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return BaselineAnnotationInput.class;
		}

		@Override
		public String getName() {
			return "baselineAnnotation";
		}
	}

	public BaselineAnnotationFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	@Override
	protected List<GraphSeries> processServiceGraph(Collection<String> serviceIds, 
			String serviceId, String viewId, String viewName,
			BaseGraphInput input, Pair<DateTime, DateTime> timeSpan, int pointsWanted) {
	
		BaselineAnnotationInput baInput = (BaselineAnnotationInput)input;
				
		Series series = createGraphSeries(baInput.text, 0);
				
		RegressionFunction regressionFunction = new RegressionFunction(apiClient, settingsMaps);
		Pair<RegressionInput, RegressionWindow> inputPair = regressionFunction.getRegressionInput(serviceId,
			viewId, input, timeSpan, false);
		
		RegressionInput regressionInput = inputPair.getFirst();
		RegressionWindow regressionWindow = inputPair.getSecond();
		
		long time = regressionWindow.activeWindowStart.getMillis();
		Object timeValue = getTimeValue(time, input);
		
		String activeWindow = prettyTime.formatDuration(DateTime.now().minusMinutes(regressionWindow.activeTimespan).toDate());
		String baselineWindow = prettyTime.formatDuration(DateTime.now().minusMinutes(regressionInput.baselineTimespan).toDate());
		
		String text = String.format(baInput.text, baselineWindow, activeWindow);
		
		series.values.add(Arrays.asList(new Object[] { timeValue, getServiceValue(text, serviceId, serviceIds) }));
		
		return Collections.singletonList(GraphSeries.of(series, 1, baInput.text));
	}

		
}
