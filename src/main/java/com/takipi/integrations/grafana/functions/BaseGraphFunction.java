package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.joda.time.DateTime;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public abstract class BaseGraphFunction extends GrafanaFunction {

	private static final int DEFAULT_POINTS = 100;

	protected static class GraphSeries {
		protected Series series;
		protected long volume;
		
		protected static GraphSeries of(Series series, long volume) {
			GraphSeries result = new GraphSeries();
			result.series = series;
			result.volume = volume;
			return result;
		}
	}
	
	protected class AsyncTask implements Callable<AsyncResult> {
		String serviceId;
		String viewId;
		String viewName;
		BaseGraphInput request;
		Pair<DateTime, DateTime> timeSpan;
		boolean hasMultipleServices;
		int pointsWanted;

		protected AsyncTask(String serviceId, String viewId, String viewName,
				BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, boolean hasMultipleServices, int pointsWanted) {

			this.serviceId = serviceId;
			this.viewId = viewId;
			this.viewName = viewName;
			this.request = request;
			this.timeSpan = timeSpan;
			this.hasMultipleServices = hasMultipleServices;
			this.pointsWanted = pointsWanted;
		}

		public AsyncResult call() {
			List<GraphSeries> serviceSeries = processServiceGraph(serviceId, viewId, viewName,
					request, timeSpan, hasMultipleServices, pointsWanted);

			return new AsyncResult(serviceSeries);
		}
	}

	protected static class AsyncResult {
		protected List<GraphSeries> data;

		protected AsyncResult(List<GraphSeries> data) {
			this.data = data;
		}
	}

	public BaseGraphFunction(ApiClient apiClient) {
		super(apiClient);
	}

	protected List<GraphSeries> processAsync(String[] serviceIds, BaseGraphInput request,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted, boolean hasMultipleServices) {

		CompletionService<AsyncResult> completionService = new ExecutorCompletionService<AsyncResult>(executor);

		int tasks = 0;

		for (String serviceId : serviceIds) {

			Map<String, String> views = getViews(serviceId, request);

			for (Map.Entry<String, String> entry : views.entrySet()) {

				String viewId = entry.getKey();
				String viewName = entry.getValue();

				tasks++;
				completionService.submit(new AsyncTask(serviceId, viewId, viewName, request,
						timeSpan, hasMultipleServices, pointsWanted));
			}
		}

		List<GraphSeries> result = new ArrayList<GraphSeries>();

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

	protected abstract List<GraphSeries> processServiceGraph(String serviceId, String viewId, String viewName,
			BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, boolean multiService, int pointsWanted);

	protected Map<String, String> getViews(String serviceId, BaseGraphInput input) {
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

	protected List<Series> processSeries(List<GraphSeries> series, BaseGraphInput request) {

		List<Series> result = new ArrayList<Series>();

		for (GraphSeries entry : series) {
			result.add(entry.series);
		}

		sortByName(result);

		return result;
	}

	private int getPointsWanted(BaseGraphInput request, Pair<DateTime, DateTime> timePair) {

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

	protected List<GraphSeries> processSync(String[] serviceIds, BaseGraphInput request,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted, boolean hasMultipleServices) {
		List<GraphSeries> series = new ArrayList<GraphSeries>();

		for (String serviceId : serviceIds) {
			
			Map<String, String> views = getViews(serviceId, request);

			for (Map.Entry<String, String> entry : views.entrySet()) {

				String viewId = entry.getKey();
				String viewName = entry.getValue();

				List<GraphSeries> serviceSeries = processServiceGraph(serviceId, viewId, viewName, request,
						timeSpan, hasMultipleServices, pointsWanted);

				series.addAll(serviceSeries);
			}
		}

		return series;
	}
	
	protected boolean isAsync(String[] serviceIds) {
		return serviceIds.length > 1;
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {
		if (!(functionInput instanceof BaseGraphInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		BaseGraphInput request = (BaseGraphInput) functionInput;

		Pair<DateTime, DateTime> timeSpan = TimeUtils.getTimeFilter(request.timeFilter);

		int pointsWanted = getPointsWanted(request, timeSpan);

		String[] serviceIds = getServiceIds(request);
		boolean hasMultipleServices = serviceIds.length > 1;

		List<GraphSeries> series;
		
		boolean async = isAsync(serviceIds);
		
		if (async) {
			series = processAsync(serviceIds, request, timeSpan, pointsWanted, hasMultipleServices);
		} else {	
			series = processSync(serviceIds, request, timeSpan, pointsWanted, hasMultipleServices);
		}
		
		List<Series> result = processSeries(series, request);

		return result;
	}
}
