package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.DeploymentsGraphInput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.util.TimeUtil;

public class DeploymentsAnnotation extends DeploymentsGraphFunction {
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new DeploymentsAnnotation(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return DeploymentsGraphInput.class;
		}

		@Override
		public String getName() {
			return "deploymentsAnnotation";
		}
	}

	public DeploymentsAnnotation(ApiClient apiClient) {
		super(apiClient);
	}
	
	@Override
	protected int getPointsWanted(BaseGraphInput input, Pair<DateTime, DateTime> timespan) {
		
		long delta = timespan.getSecond().minus(timespan.getFirst().getMillis()).getMillis();
		long days = TimeUnit.MILLISECONDS.toDays(delta) ;
		
		if (days > 1) {
			return (int)days;
		} else {
			return (int)TimeUnit.DAYS.toHours(1);
		}
	}

	@Override
	protected SeriesVolume processGraphPoints(String serviceId, 
			Pair<DateTime, DateTime> timeSpan, Graph graph, GraphInput input) {
	
		List<List<Object>> values = new ArrayList<List<Object>>(graph.points.size());
		
		for (GraphPoint gp : graph.points) {
			
			if ((gp.stats.hits > 0) || (gp.stats.invocations > 0)) {
				DateTime gpTime = TimeUtil.getDateTime(gp.time);
				values.add(Arrays.asList(new Object[] { Long.valueOf(gpTime.getMillis()), input.deployments }));
				
				break;
			}
		}
		
		return SeriesVolume.of(values,0);
	}
}
