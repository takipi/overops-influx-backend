package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

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
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class GraphFunction extends BaseVolumeFunction {

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
	
	protected class AsyncTask implements Callable<AsyncResult> {
		GraphFunction graphFunction;
		String serviceId;
		String viewId;
		String viewName;
		GraphInput request;
		Pair<String, String> timeSpan;
		boolean hasMultipleServices;
		int pointsWanted;

		protected AsyncTask(GraphFunction graphFunction, String serviceId, String viewId, String viewName,
				GraphInput request, Pair<String, String> timeSpan, boolean hasMultipleServices, int pointsWanted) {

			this.graphFunction = graphFunction;
			this.serviceId = serviceId;
			this.viewId = viewId;
			this.viewName = viewName;
			this.request = request;
			this.timeSpan = timeSpan;
			this.hasMultipleServices = hasMultipleServices;
			this.pointsWanted = pointsWanted;
		}

		public AsyncResult call() {
			List<Pair<Series, Long>> serviceSeries = graphFunction.processServiceGraph(serviceId, viewId, viewName,
					request, timeSpan, hasMultipleServices, pointsWanted);

			return new AsyncResult(serviceSeries);
		}
	}

	protected static class AsyncResult {
		protected List<Pair<Series, Long>> data;

		protected AsyncResult(List<Pair<Series, Long>> data) {
			this.data = data;
		}
	}

	public GraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

	protected List<Pair<Series, Long>> processASync(String[] serviceIds, GraphInput request,
			Pair<String, String> timeSpan, int pointsWanted, boolean hasMultipleServices) {

		CompletionService<AsyncResult> completionService = new ExecutorCompletionService<AsyncResult>(executor);

		int tasks = 0;

		for (String serviceId : serviceIds) {

			Map<String, String> views = getViews(serviceId, request);

			for (Map.Entry<String, String> entry : views.entrySet()) {

				String viewId = entry.getKey();
				String viewName = entry.getValue();

				tasks++;
				completionService.submit(new AsyncTask(this, serviceId, viewId, viewName, request,
						timeSpan, hasMultipleServices, pointsWanted));
			}
		}

		List<Pair<Series, Long>> result = new ArrayList<Pair<Series, Long>>();

		int received = 0;

		while (received < tasks) {			
			try {
				Future<AsyncResult> future = completionService.take();
				received++;
				AsyncResult asynResult = future.get();
				result.addAll(asynResult.data);
			} catch (Exception e) {
				throw new IllegalStateException(e);
			} 
		}

		return result;
	}

	private List<Pair<Series, Long>> processServiceGraph(String serviceId, String viewId, String viewName,
			GraphInput request, Pair<String, String> timeSpan, boolean multiService, int pointsWanted) {

		GraphRequest.Builder builder = GraphRequest.newBuilder().setServiceId(serviceId).setViewId(viewId)
				.setGraphType(request.graphType).setVolumeType(request.volumeType).setFrom(timeSpan.getFirst())
				.setTo(timeSpan.getSecond()).setWantedPointCount(pointsWanted);

		applyFilters(request, serviceId, builder);

		Response<GraphResult> response = apiClient.get(builder.build());

		if ((response.isBadResponse()) || (response.data == null)) {
			throw new IllegalStateException("GraphResult code " + response.responseCode);
		}

		List<Pair<Series, Long>> result = new ArrayList<Pair<Series, Long>>(response.data.graphs.size());

		for (Graph graph : response.data.graphs) {
			Series series = new Series();

			if (multiService) {
				series.name = viewName + SERVICE_SEPERATOR + serviceId;
			} else {
				series.name = viewName;
			}

			series.columns = Arrays.asList(new String[] { TIME_COLUMN, request.volumeType.toString() });

			if (response.data.graphs.size() > 1) {
				series.tags = Collections.singletonList(graph.id);
			}

			Pair<List<List<Object>>, Long> seriesData = processGraphPoints(series, graph, request);

			series.values = seriesData.getFirst();
			Long seriesVolume = seriesData.getSecond();

			result.add(Pair.of(series, seriesVolume));
		}

		return result;
	}

	private Pair<List<List<Object>>, Long> processGraphPoints(Series series, Graph graph, GraphInput request) {

		long volume = 0;
		List<List<Object>> values = new ArrayList<List<Object>>(graph.points.size());

		for (GraphPoint gp : graph.points) {
			DateTime gpTime = ISODateTimeFormat.dateTimeParser().parseDateTime(gp.time);
			long value;

			if (request.volumeType.equals(VolumeType.invocations)) {
				value = gp.stats.invocations;
			} else {
				value = gp.stats.hits;
			}

			volume += value;
			values.add(Arrays.asList(new Object[] { Long.valueOf(gpTime.getMillis()), value }));
		}

		return Pair.of(values, Long.valueOf(volume));
	}

	protected Map<String, String> getViews(String serviceId, GraphInput input) {
		String viewId = getViewId(serviceId, input.view);

		if (viewId != null) {
			return Collections.singletonMap(viewId, input.view);
		} else {
			return Collections.emptyMap();
		}
	}

	protected void sortByName(List<Series> seriesList) {

		seriesList.sort(new Comparator<Series>() {

			@Override
			public int compare(Series o1, Series o2) {
				Series s1 = (Series) o1;
				Series s2 = (Series) o2;

				return s1.name.compareTo(s2.name);
			}
		});
	}

	protected List<Series> processSeries(List<Pair<Series, Long>> series, GraphInput request) {

		List<Series> result = new ArrayList<Series>();

		for (Pair<Series, Long> entry : series) {
			result.add(entry.getFirst());
		}

		sortByName(result);

		return result;
	}

	private int getPointsWanted(GraphInput request, Pair<DateTime, DateTime> timePair) {

		int result;

		if (request.interval > 0) {
			long to = timePair.getSecond().getMillis();
			long from = timePair.getFirst().getMillis();
			result = (int) ((to - from) / request.interval);
		} else {
			result = DEFAULT_POINTS;
		}

		return result;
	}

	protected List<Pair<Series, Long>> processSync(String[] serviceIds, GraphInput request,
			Pair<String, String> timeSpan, int pointsWanted, boolean hasMultipleServices) {
		List<Pair<Series, Long>> series = new ArrayList<Pair<Series, Long>>();

		for (String serviceId : serviceIds) {

			Map<String, String> views = getViews(serviceId, request);

			for (Map.Entry<String, String> entry : views.entrySet()) {

				String viewId = entry.getKey();
				String viewName = entry.getValue();

				List<Pair<Series, Long>> serviceSeries = processServiceGraph(serviceId, viewId, viewName, request,
						timeSpan, hasMultipleServices, pointsWanted);

				series.addAll(serviceSeries);
			}
		}

		return series;
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {
		if (!(functionInput instanceof GraphInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		super.process(functionInput);

		GraphInput request = (GraphInput) functionInput;

		Pair<DateTime, DateTime> timePair = TimeUtils.getTimeFilter(request.timeFilter);
		Pair<String, String> timeSpan = TimeUtils.toTimespan(timePair);

		int pointsWanted = getPointsWanted(request, timePair);

		String[] serviceIds = getServiceIds(request);
		boolean hasMultipleServices = serviceIds.length > 1;

		List<Pair<Series, Long>> series = processASync(serviceIds, request, timeSpan, pointsWanted, hasMultipleServices);
		List<Series> result = processSeries(series, request);

		return result;
	}
}
