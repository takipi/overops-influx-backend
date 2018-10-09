package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.metrics.Graph;
import com.takipi.common.api.data.metrics.Graph.GraphPoint;
import com.takipi.common.api.request.metrics.GraphRequest;
import com.takipi.common.api.result.metrics.GraphResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.common.api.util.Pair;
import com.takipi.common.api.util.ValidationUtil.VolumeType;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GraphInput;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class GraphFunction extends GrafanaFunction {
	
	private static final int DEFAULT_POINTS = 100;
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new GraphFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass() {
			return GraphInput.class;
		}
		
		@Override
		public String getName() {
			return "graph";
		}
	}
	
	public GraphFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	private List<Series> processServiceGraph(String serviceId, String viewId, String viewName, GraphInput request,
			Pair<String, String> timeSpan, boolean multiService, int pointsWanted) {
		
		GraphRequest.Builder builder = GraphRequest.newBuilder().setServiceId(serviceId).setViewId(viewId)
				.setGraphType(request.graphType).setVolumeType(request.volumeType).setFrom(timeSpan.getFirst())
				.setTo(timeSpan.getSecond()).setWantedPointCount(pointsWanted);

		applyFilters(request, serviceId, builder);

		Response<GraphResult> response = apiClient.get(builder.build());

		if ((response.isBadResponse()) || (response.data == null)) {
			throw new IllegalStateException("GraphResult code " + response.responseCode);
		}

		List<Series> result = new ArrayList<Series>(response.data.graphs.size());

		for (Graph graph : response.data.graphs) {
			Series series = new Series();
			
			if (multiService) {
				series.name = viewName  + SERVICE_SEPERATOR + serviceId;
			}
			else {
				series.name = viewName;
			}
		
			series.values = new ArrayList<List<Object>>(graph.points.size());
			series.columns = Arrays.asList(new String[] { TIME_COLUMN, SUM_COLUMN });
			
			if (response.data.graphs.size() > 1) {
				series.tags = Collections.singletonList(graph.id);
			}

			for (GraphPoint gp : graph.points) {
				DateTime gpTime = ISODateTimeFormat.dateTimeParser().parseDateTime(gp.time);
				long value;

				if (request.volumeType.equals(VolumeType.invocations)) {
					value = gp.stats.invocations;
				} else {
					value = gp.stats.hits;
				}

				series.values.add(Arrays.asList(new Object[] { Long.valueOf(gpTime.getMillis()), value }));
			}

			result.add(series);
		}

		return result;
	}
	
	protected Map<String, String> getViews(String serviceId, GraphInput input) {
		String viewId = getViewId(serviceId, input);
		
		if (viewId != null) {
			return Collections.singletonMap(viewId, input.view);
		} else {
			return Collections.emptyMap();
		}
	}
	
	@Override
	public QueryResult process(FunctionInput functionInput) {  
		if (!(functionInput instanceof GraphInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		GraphInput request = (GraphInput)functionInput;

		Pair<DateTime, DateTime> timePair = TimeUtils.getTimeFilter(request.timeFilter);
		Pair<String, String> timeSpan = TimeUtils.toTimespan(timePair);

		int pointsWanted;
		
		if (request.interval > 0) {
			long to = timePair.getSecond().getMillis() ;
			long from = timePair.getFirst().getMillis();
			pointsWanted = (int)((to -from) / request.interval);
		} else {
			pointsWanted = DEFAULT_POINTS;
		}
		
		String[] services = getServiceIds(request);

		List<Series> series = new ArrayList<Series>();

		for (String serviceId : services) {
			
			Map<String, String> views = getViews(serviceId, request);
			
			for (Map.Entry<String,String> entry : views.entrySet()) {
				List<Series> serviceSeries = processServiceGraph(serviceId, entry.getKey(), entry.getValue(), request, timeSpan, services.length > 1, pointsWanted);
				series.addAll(serviceSeries);
			}
		}

		return createQueryResults(series);
	}
}
