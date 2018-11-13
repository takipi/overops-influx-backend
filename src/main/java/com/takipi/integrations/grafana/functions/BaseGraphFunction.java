package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.TimeUtil;

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
	
	protected class GraphAsyncTask extends BaseAsyncTask implements Callable<AsyncResult> {
		String serviceId;
		String viewId;
		String viewName;
		BaseGraphInput request;
		Pair<DateTime, DateTime> timeSpan;
		String[] serviceIds;
		int pointsWanted;

		protected GraphAsyncTask(String serviceId, String viewId, String viewName,
				BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, String[] serviceIds, int pointsWanted) {

			this.serviceId = serviceId;
			this.viewId = viewId;
			this.viewName = viewName;
			this.request = request;
			this.timeSpan = timeSpan;
			this.serviceIds = serviceIds;
			this.pointsWanted = pointsWanted;
		}

		@Override
		public AsyncResult call() {
			beforeCall();
			
			try {
				List<GraphSeries> serviceSeries = processServiceGraph(serviceId, viewId, viewName,
						request, timeSpan, serviceIds, pointsWanted);
	
				return new AsyncResult(serviceSeries);
			}
			finally {
				afterCall();
			}
		}
		
		@Override
		public String toString() {
			return String.join(" ", "Graph", serviceId, viewId, viewName, 
				timeSpan.toString(), String.valueOf(pointsWanted));
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

	@Override
	protected String getSeriesName(BaseGraphInput input, String seriesName,
			Object volumeType, String serviceId, String[] serviceIds) {
		String tagName;
		
		if (seriesName != null) {
			tagName = seriesName;
		} else {
			tagName = volumeType.toString();	
		}
		
		String result = getServiceValue(tagName, serviceId, serviceIds);
		
		return result;
	}
	
	protected Collection<GraphAsyncTask> getTasks(String[] serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {
		
		List<GraphAsyncTask> result = new ArrayList<GraphAsyncTask>();
		
		for (String serviceId : serviceIds) {

			Map<String, String> views = getViews(serviceId, input);

			for (Map.Entry<String, String> entry : views.entrySet()) {

				String viewId = entry.getKey();
				String viewName = entry.getValue();

				result.add(new GraphAsyncTask(serviceId, viewId, viewName, input,
						timeSpan, serviceIds, pointsWanted));
			}
		}
		
		return result;
	}
	
	protected List<GraphSeries> processAsync(String[] serviceIds, BaseGraphInput request,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {

		CompletionService<AsyncResult> completionService = new ExecutorCompletionService<AsyncResult>(GrafanaThreadPool.executor);

		Collection<GraphAsyncTask> tasks = getTasks(serviceIds, request, timeSpan, pointsWanted);
		
		for (GraphAsyncTask task : tasks) {
			completionService.submit(task);
		}

		List<GraphSeries> result = new ArrayList<GraphSeries>();

		int received = 0;

		while (received < tasks.size()) {			
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
			BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, String[] serviceIds, int pointsWanted);

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
				return o1.name.compareTo(o2.name);
			}
		});
	}

	protected List<Series> processSeries(List<GraphSeries> series,
			@SuppressWarnings("unused") BaseGraphInput input) {
		List<Series> result = new ArrayList<Series>();

		for (GraphSeries entry : series) {
			result.add(entry.series);
		}

		sortByName(result);

		return result;
	}

	protected int getPointsWanted(BaseGraphInput input, Pair<DateTime, DateTime> timespan) {

		if (input.pointsWanted > 0) {
			return input.pointsWanted;
		}
		
		int result;

		if (input.interval > 0) {
			long to = timespan.getSecond().getMillis();
			long from = timespan.getFirst().getMillis();
			result = (int) ((to - from) / input.interval);
		} else {
			result = DEFAULT_POINTS;
		}

		return result;
	}

	protected List<GraphSeries> processSync(String[] serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {
		
		List<GraphSeries> series = new ArrayList<GraphSeries>();
		
		Collection<GraphAsyncTask> tasks = getTasks(serviceIds, input, timeSpan, pointsWanted);
		
		for (GraphAsyncTask task : tasks) {
			List<GraphSeries> taksData = task.call().data;
			
			if (taksData != null) {
				series.addAll(taksData);
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

		BaseGraphInput input = (BaseGraphInput) functionInput;

		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);

		int pointsWanted = getPointsWanted(input, timeSpan);

		String[] serviceIds = getServiceIds(input);

		List<GraphSeries> series;
		
		boolean async = isAsync(serviceIds);
		
		if (async) {
			series = processAsync(serviceIds, input, timeSpan, pointsWanted);
		} else {	
			series = processSync(serviceIds, input, timeSpan, pointsWanted);
		}
		
		List<Series> result = processSeries(series, input);

		return result;
	}
}
