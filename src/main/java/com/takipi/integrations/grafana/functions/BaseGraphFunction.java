package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.TimeUtil;

public abstract class BaseGraphFunction extends GrafanaFunction {

	private static final int DEFAULT_POINTS = 100;

	protected static class GraphData {
		protected Map<Long, Long> points;
		protected long volume;
		protected String key;
		
		protected GraphData(String key) {
			this.key = key;
			this.points = new TreeMap<Long, Long>();
		}
		
		@Override
		public boolean equals(Object obj)
		{
			return Objects.equal(key, ((GraphData)obj).key);
		}
		
		@Override
		public int hashCode()
		{
			return key.hashCode();
		}
	}
	
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
	
	protected class GraphAsyncTask extends BaseAsyncTask implements Callable<Object>{
		String serviceId;
		String viewId;
		String viewName;
		BaseGraphInput request;
		Pair<DateTime, DateTime> timeSpan;
		Collection<String> serviceIds;
		int pointsWanted;

		protected GraphAsyncTask(String serviceId, String viewId, String viewName,
				BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds, int pointsWanted) {

			this.serviceId = serviceId;
			this.viewId = viewId;
			this.viewName = viewName;
			this.request = request;
			this.timeSpan = timeSpan;
			this.serviceIds = serviceIds;
			this.pointsWanted = pointsWanted;
		}

		@Override
		public Object call() {
			
			beforeCall();
			
			try {
				List<GraphSeries> serviceSeries = processServiceGraph(serviceIds, serviceId, viewId, viewName,
						request, timeSpan, pointsWanted);
	
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
		Object volumeType, String serviceId, Collection<String> serviceIds) {
		String tagName;
		
		if (seriesName != null) {
			tagName = seriesName;
		} else {
			tagName = volumeType.toString();	
		}
				
		String result = getServiceValue(tagName, serviceId, serviceIds);
		
		return result;
	}
	
	protected static GraphSeries getGraphSeries(GraphData graphData, String name) {
		
		GraphSeries result = new GraphSeries();
					
		result.series = new Series();
		
		result.series.name = EMPTY_NAME;
		result.series.columns = Arrays.asList(new String[] { TIME_COLUMN, name });
		result.series.values = new ArrayList<List<Object>>();
		result.volume = graphData.volume;
			
		for (Map.Entry<Long, Long> graphPoint : graphData.points.entrySet()) {				
			result.series.values.add(Arrays.asList(new Object[] { graphPoint.getKey(), graphPoint.getValue() }));
		}
		
		return result;
	}
	
	protected List<Series> limitSeries(List<GraphSeries> series, int limit) {
		
		List<GraphSeries> limitedGraphSeries = limitGraphSeries(series, limit);
		List<Series> result = new ArrayList<Series>(limitedGraphSeries.size());
		
		for (GraphSeries graphSeries : limitedGraphSeries) {
			result.add(graphSeries.series);
		}
		
		return result;
	}
	
	protected List<GraphSeries> limitGraphSeries(List<GraphSeries> series, int limit) {
		
		List<GraphSeries> sorted = new ArrayList<GraphSeries>(series);
		
		sortSeriesByVolume(sorted);
		
		List<GraphSeries> result = new ArrayList<GraphSeries>();
		
		for (int i = 0; i < Math.min(limit, sorted.size()); i++) {
			
			GraphSeries graphSeries = sorted.get(i);
			
			if (graphSeries.volume > 0) {
				result.add(graphSeries);
			}
		}
		return result;
	}
	
	protected void sortSeriesByVolume(List<GraphSeries> series) {
		series.sort(new Comparator<GraphSeries>() {

			@Override
			public int compare(GraphSeries o1, GraphSeries o2) {
				return (int)(o2.volume - o1.volume);
			}
		});
	}
	
	protected Collection<Callable<Object>> getTasks(Collection<String> serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {
		
		List<Callable<Object>> result = new ArrayList<Callable<Object>>();
		
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
	
	protected List<GraphSeries> processAsync(Collection<String> serviceIds, BaseGraphInput request,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {
 
		Collection<Callable<Object>> tasks = getTasks(serviceIds, request, timeSpan, pointsWanted);
		List<Object> taskResults = executeTasks(tasks, true);
		
		List<GraphSeries> result = new ArrayList<GraphSeries>();
		
		for (Object taskResult : taskResults) {
			
			if (taskResult instanceof AsyncResult) {
				AsyncResult asyncResult = (AsyncResult)taskResult;
				
				if (asyncResult.data != null) {
					result.addAll(asyncResult.data);
				}
			}
		}
		
		return result;
	}

	protected abstract List<GraphSeries> processServiceGraph(Collection<String> serviceIds, String serviceId, 
			String viewId, String viewName, BaseGraphInput request, Pair<DateTime, DateTime> timeSpan, int pointsWanted);

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

	/**
	 * @param input - needed by child classes 
	 */
	protected List<Series> processSeries(List<GraphSeries> series, BaseGraphInput input) {

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

	protected List<GraphSeries> processSync(Collection<String> serviceIds, BaseGraphInput input,
			Pair<DateTime, DateTime> timeSpan, int pointsWanted) {
		
		List<GraphSeries> series = new ArrayList<GraphSeries>();
		
		Collection<Callable<Object>> tasks = getTasks(serviceIds, input, timeSpan, pointsWanted);
		
		for (Callable<Object> task : tasks) {
			
			AsyncResult taskResult;
			try {
				taskResult = (AsyncResult)(task.call());
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
			
			if (taskResult.data != null) {
				series.addAll(taskResult.data);
			}
		}
		
		return series;
	}
	
	protected boolean isAsync(Collection<String> serviceIds) {
		return serviceIds.size() > 1;
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {
		if (!(functionInput instanceof BaseGraphInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		BaseGraphInput input = (BaseGraphInput) functionInput;

		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);

		int pointsWanted = getPointsWanted(input, timeSpan);

		Collection<String> serviceIds = getServiceIds(input);

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
