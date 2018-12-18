package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
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
	protected List<GraphSeries> processServiceGraph(String serviceId, String viewId, String viewName,
			BaseGraphInput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds, int pointsWanted) {
	
		BaselineAnnotationInput baInput = (BaselineAnnotationInput)input;
		
		GraphSeries result = new GraphSeries();
		
		result.series = new Series();
		
		result.series.name = EMPTY_NAME;
		result.series.columns = Arrays.asList(new String[] { TIME_COLUMN, baInput.text });
		result.series.values = new ArrayList<List<Object>>();
		result.volume = 1;
		
		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		Pair<RegressionInput, RegressionWindow> inputPair = regressionFunction.getRegressionInput(serviceId, viewId, input, timeSpan);
		
		long time = inputPair.getSecond().activeWindowStart.getMillis();
		
		result.series.values.add(Arrays.asList(new Object[] { time, getServiceValue(baInput.text, serviceId, serviceIds) }));
		
		return Collections.singletonList(result);
	}

		
}
