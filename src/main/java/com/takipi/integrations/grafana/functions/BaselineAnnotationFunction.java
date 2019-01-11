package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.BaselineAnnotationInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.TimeUtil;

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
		
		GraphSeries result = new GraphSeries();
		
		result.series = new Series();
		
		result.series.name = EMPTY_NAME;
		result.series.columns = Arrays.asList(new String[] { TIME_COLUMN, baInput.text });
		result.series.values = new ArrayList<List<Object>>();
		result.volume = 1;
		
		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		Pair<RegressionInput, RegressionWindow> inputPair = regressionFunction.getRegressionInput(serviceId, viewId, input, timeSpan);
		
		RegressionInput regressionInput = inputPair.getFirst();
		RegressionWindow regressionWindow = inputPair.getSecond();
		
		long time = regressionWindow.activeWindowStart.getMillis();
		
		String activeWindow = TimeUtil.getTimeInterval(TimeUnit.MINUTES.toMillis(regressionWindow.activeTimespan));
		String baselineWindow = TimeUtil.getTimeInterval(TimeUnit.MINUTES.toMillis(regressionInput.baselineTimespan));

		String text = String.format(baInput.text, baselineWindow, activeWindow);
		
		result.series.values.add(Arrays.asList(new Object[] { time, getServiceValue(text, serviceId, serviceIds) }));
		
		return Collections.singletonList(result);
	}

		
}
