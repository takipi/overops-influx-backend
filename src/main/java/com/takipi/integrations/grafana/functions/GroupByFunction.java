package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;

import org.joda.time.DateTime;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.request.volume.EventsVolumeRequest;
import com.takipi.common.api.result.event.EventResult;
import com.takipi.common.api.result.volume.EventsVolumeResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.common.api.util.Pair;
import com.takipi.common.api.util.ValidationUtil.VolumeType;
import com.takipi.common.udf.util.ApiFilterUtil;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GroupByInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class GroupByFunction extends BaseVolumeFunction {

	public enum AggregationField {
		type, name, location, entryPoint, label, introduced_by, application, server, deployment;
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new GroupByFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return GroupByInput.class;
		}

		@Override
		public String getName() {
			return "groupBy";
		}
	}

	protected static class EventVolume {
		protected long sum;
		protected long count;
		protected Comparable<Object> compareBy;
		protected String time;

		public static EventVolume of(Comparable<Object> compareBy, String time) {
			EventVolume result = new EventVolume();
			result.compareBy = compareBy;
			result.time = time;

			return result;
		}
	}

	protected class BaseAsyncTask implements Callable<AsyncResult> {
		public Map<String, EventVolume> map;

		protected BaseAsyncTask(Map<String, EventVolume> map) {
			this.map = map;
		}

		@Override
		public AsyncResult call() throws Exception {
			return null;
		}
	}

	protected class AsyncResult {
	}

	protected class EventAsyncTask extends BaseAsyncTask {

		protected String serviceId;
		protected GroupByInput request;
		protected Pair<String, String> timeSpan;

		protected EventAsyncTask(Map<String, EventVolume> map, String serviceId, GroupByInput request,
				Pair<String, String> timeSpan) {
			super(map);
			this.serviceId = serviceId;
			this.request = request;
			this.timeSpan = timeSpan;
		}

		@Override
		public AsyncResult call() throws Exception {

			List<EventResult> events = getEventList(serviceId, request, timeSpan);

			for (EventResult event : events) {
				processEventGroupBy(request, map, event, timeSpan.getFirst());
			}

			return null;
		}
	}

	protected class FilterAsyncTask extends BaseAsyncTask {
		protected String key;
		protected GroupByInput request;
		protected String serviceId;
		protected String viewId;
		protected Pair<String, String> timeSpan;
		protected Collection<String> applications;
		protected Collection<String> servers;
		protected Collection<String> deployments;

		protected FilterAsyncTask(Map<String, EventVolume> map, String key, GroupByInput request,
				String serviceId, String viewId, Pair<String, String> timeSpan, Collection<String> applications,
				Collection<String> servers, Collection<String> deployments) {

			super(map);
			this.key = key;
			this.request = request;
			this.serviceId = serviceId;
			this.viewId = viewId;
			this.timeSpan = timeSpan;
			this.applications = applications;
			this.servers = servers;
			this.deployments = deployments;
		}

		@Override
		public AsyncResult call() throws Exception {
			executeVolumeRequest(map, key, request, serviceId, viewId, timeSpan, applications, servers, deployments);
			return null;
		}
	}

	public GroupByFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private static void updateMap(Map<String, EventVolume> map, String key, String time, long value) {
		updateMap(map, key, time, value, null);
	}

	private static void updateMap(Map<String, EventVolume> map, String key, String time, long value,
			Comparable<Object> compareBy) {

		if (key == null) {
			return;
		}

		synchronized (map) {	
			EventVolume stat = map.get(key);
	
			if (stat == null) {
				stat = EventVolume.of(compareBy, time);
				map.put(key, stat);
			}
	
			stat.sum = stat.sum + value;
			stat.count = stat.count + 1;
	
			if (stat.compareBy != null) {
				int compareResult = stat.compareBy.compareTo(compareBy);
	
				if (compareResult > 0) {
					stat.compareBy = compareBy;
				}
			} else {
				stat.compareBy = compareBy;
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void processEventGroupBy(GroupByInput request, Map<String, EventVolume> map, EventResult event,
			String time) {

		long value;

		if (request.volumeType.equals(VolumeType.invocations)) {
			value = event.stats.invocations;
		} else {
			value = event.stats.hits;
		}

		switch (request.field) {
		case type:
			updateMap(map, event.type, time, value);
			break;

		case name:
			updateMap(map, event.name, time, value);
			break;

		case location:
			updateMap(map, event.error_location.prettified_name, time, value);
			break;

		case entryPoint:
			updateMap(map, event.entry_point.prettified_name, time, value);
			break;

		case label:
			if (event.labels == null) {
				return;
			}

			for (String label : event.labels) {
				updateMap(map, label, time, value);
			}

			break;

		case introduced_by:
			Comparable compareBy = TimeUtils.getDateTime(event.first_seen);
			updateMap(map, event.introduced_by, time, value, compareBy);
			break;

		default:
			throw new IllegalStateException("Unsupported grouping " + request.field);
		}
	}

	private List<BaseAsyncTask> processEventsGroupBy(Map<String, EventVolume> map, GroupByInput request,
			String serviceId, Pair<String, String> timeSpan) {

		return Collections.singletonList(new EventAsyncTask(map, serviceId, request, timeSpan));
	}

	private List<BaseAsyncTask> processApplicationsGroupBy(Map<String, EventVolume> map, GroupByInput request,
			String serviceId, Pair<String, String> timeSpan) {

		List<BaseAsyncTask> result = new ArrayList<BaseAsyncTask>();

		String viewId = getViewId(serviceId, request.view);

		if (viewId == null) {
			return result;
		}

		Collection<String> applications;

		if (request.hasApplications()) {
			applications = request.getApplications(serviceId);
		} else {
			applications = ApiFilterUtil.getApplications(apiClient, serviceId);
		}

		for (String application : applications) {

			result.add(new FilterAsyncTask(map, application, request, serviceId, viewId, timeSpan,
					Collections.singleton(application), request.getServers(serviceId),
					request.getDeployments(serviceId)));

		}

		return result;
	}

	private List<BaseAsyncTask> processServersGroupBy(Map<String, EventVolume> map, GroupByInput request,
			String serviceId, Pair<String, String> timeSpan) {

		List<BaseAsyncTask> result = new ArrayList<BaseAsyncTask>();

		String viewId = getViewId(serviceId, request.view);

		if (viewId == null) {
			return result;
		}

		Collection<String> servers;

		if (request.hasServers()) {
			servers = request.getServers(serviceId);
		} else {
			servers = ApiFilterUtil.getSevers(apiClient, serviceId);
		}

		for (String server : servers) {

			result.add(new FilterAsyncTask(map, server, request, serviceId, viewId, timeSpan,
					request.getApplications(serviceId), Collections.singleton(server),
					request.getDeployments(serviceId)));
		}

		return result;
	}

	private void executeVolumeRequest(Map<String, EventVolume> map, String key, GroupByInput request, String serviceId,
			String viewId, Pair<String, String> timeSpan, Collection<String> applications, Collection<String> servers,
			Collection<String> deployments) {

		EventsVolumeRequest.Builder builder = EventsVolumeRequest.newBuilder().setServiceId(serviceId)
				.setFrom(timeSpan.getFirst()).setTo(timeSpan.getSecond()).setViewId(viewId)
				.setVolumeType(request.volumeType);

		for (String app : applications) {
			builder.addApp(app);
		}

		for (String dep : deployments) {
			builder.addDeployment(dep);
		}

		for (String server : servers) {
			builder.addServer(server);
		}

		Response<EventsVolumeResult> response = apiClient.get(builder.build());

		if ((response.isBadResponse()) || (response.data == null)) {
			throw new IllegalStateException("Could not acquire volume for app / srv/ dep " + applications + "/"
					+ servers + "/" + deployments + " in service " + serviceId + " . Error " + response.responseCode);
		}

		updateMap(map, key, timeSpan.getFirst(), response.data.events, request.volumeType);
	}

	private List<BaseAsyncTask> processDeploymentsGroupBy(Map<String, EventVolume> map, GroupByInput request,
			String serviceId, Pair<String, String> timeSpan) {

		List<BaseAsyncTask> result = new ArrayList<BaseAsyncTask>();

		String viewId = getViewId(serviceId, request.view);

		if (viewId == null) {
			return result;
		}

		Collection<String> deployments;

		if (request.hasDeployments()) {
			deployments = request.getDeployments(serviceId);
		} else {
			deployments = ApiFilterUtil.getDeployments(apiClient, serviceId);
		}

		for (String deployment : deployments) {

			result.add(new FilterAsyncTask(map, deployment, request, serviceId, viewId, timeSpan,
					request.getApplications(serviceId), request.getServers(serviceId),
					Collections.singleton(deployment)));

		}

		return result;
	}

	private void updateMap(Map<String, EventVolume> map, String key, String time, List<EventResult> events,
			VolumeType volumeType) {
		if (events == null) {
			return;
		}

		for (EventResult event : events) {
			long value;

			if (volumeType.equals(VolumeType.invocations)) {
				value = event.stats.invocations;
			} else {
				value = event.stats.hits;
			}

			updateMap(map, key, time, value);
		}
	}

	private void updateGroupResutMap(Map<String, GroupResult> map, String serviceId, String groupByKey,
			EventVolume eventVolume) {
		GroupResult groupResult = map.get(groupByKey);

		if (groupResult == null) {
			groupResult = new GroupResult(serviceId);
			map.put(groupByKey, groupResult);
		}

		groupResult.updateCompareBy(eventVolume.compareBy);
		groupResult.addVolume(eventVolume);
	}

	private Map<String, GroupResult> processServiceGroupBy(String serviceId, GroupByInput request,
			Collection<Pair<String, String>> timeSpans) {

		int tasks = 0;
		CompletionService<AsyncResult> completionService = new ExecutorCompletionService<AsyncResult>(executor);
		List<Map<String, EventVolume>> outputMaps = new ArrayList<Map<String, EventVolume>>();

		for (Pair<String, String> timespan : timeSpans) {
			
			Map<String, EventVolume> outputMap = new HashMap<String, EventVolume>();
			outputMaps.add(outputMap);
			
			List<BaseAsyncTask> serviceTasks = processServiceGroupBy(outputMap, serviceId, request, timespan);
			tasks += serviceTasks.size();
			
			for (BaseAsyncTask task : serviceTasks) {
				completionService.submit(task);
			}
		}

		int received = 0;

		while (received < tasks) {
			try {
				completionService.take();
				received++;
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}

		Map<String, GroupResult> result = new HashMap<String, GroupResult>();

		for (Map<String, EventVolume> outputMap : outputMaps) {
			for (Map.Entry<String, EventVolume> entry : outputMap.entrySet()) {

				String groupByKey = entry.getKey();
				EventVolume eventVolume = entry.getValue();

				updateGroupResutMap(result, serviceId, groupByKey, eventVolume);
			}
		}

		return result;
	}

	private List<BaseAsyncTask> processServiceGroupBy(Map<String, EventVolume> map, String serviceId,
			GroupByInput request, Pair<String, String> timeSpan) {

		switch (request.field) {
		case application:
			return processApplicationsGroupBy(map, request, serviceId, timeSpan);

		case deployment:
			return processDeploymentsGroupBy(map, request, serviceId, timeSpan);

		case server:
			return processServersGroupBy(map, request, serviceId, timeSpan);

		default:
			return processEventsGroupBy(map, request, serviceId, timeSpan);
		}
	}

	private static class SeriesResult {
		private Series series;
		private Comparable<Object> maxValue;

		public static SeriesResult of(Series series, Comparable<Object> maxValue) {
			SeriesResult result = new SeriesResult();
			result.series = series;
			result.maxValue = maxValue;
			return result;
		}
	}

	protected static class GroupResult {
		private List<EventVolume> volumes;
		private String serviceId;
		private Comparable<Object> compareBy;

		public GroupResult(String serviceId) {
			this.serviceId = serviceId;
			this.volumes = new ArrayList<EventVolume>();
		}

		public void addVolume(EventVolume volume) {
			volumes.add(volume);
		}

		public Collection<EventVolume> getVolumes() {
			return volumes;
		}

		public void updateCompareBy(Comparable<Object> compareBy) {
			if (this.compareBy == null) {
				this.compareBy = compareBy;
			} else if (this.compareBy.compareTo(compareBy) < 0) {
				this.compareBy = compareBy;
			}
		}

		public Comparable<Object> getCompareBy() {
			return compareBy;
		}

		public String getServiceId() {
			return serviceId;
		}
	}

	private Collection<Pair<String, String>> getTimeSpans(GroupByInput request) {

		Pair<DateTime, DateTime> timeSpan = TimeUtils.getTimeFilter(request.timeFilter);

		if ((request.interval == null) || (request.interval.length() == 0)) {
			return Collections.singleton(TimeUtils.toTimespan(timeSpan));
		}

		long milliInterval = TimeUtils.parseInterval(request.interval) * 1000 * 60;

		List<Pair<String, String>> result = new ArrayList<Pair<String, String>>();

		DateTime startTime = timeSpan.getFirst();
		DateTime endTime;

		while (startTime.isBefore(timeSpan.getSecond())) {
			endTime = startTime.plus(milliInterval);
			result.add(TimeUtils.toTimespan(Pair.of(startTime, endTime)));
			startTime = endTime;
		}

		return result;
	}

	private static Object getEventVolumeValue(GroupByInput request, EventVolume eventVolume) {
		Object value;

		if (request.type.equals(AggregationType.sum)) {
			value = Long.valueOf(eventVolume.sum);
		} else {
			value = Double.valueOf(eventVolume.sum / eventVolume.count);
		}

		return value;
	}

	private static Series createSeries(GroupByInput request, String seriesKey) {
		Series series = new Series();

		if (request.addTags) {
			series.name = SERIES_NAME;
			series.tags = Collections.singletonList(seriesKey);
			series.columns = Arrays.asList(new String[] { TIME_COLUMN, SUM_COLUMN });
		} else {
			series.name = EMPTY_NAME;
			series.columns = Arrays.asList(new String[] { TIME_COLUMN, seriesKey });
		}

		series.values = new ArrayList<List<Object>>();

		return series;
	}

	private static Collection<SeriesResult> processGroupResults(GroupByInput request,
			Map<String, GroupResult> groupResults, boolean multiServices) {

		Map<String, SeriesResult> resultMap = new HashMap<String, SeriesResult>();

		for (Map.Entry<String, GroupResult> entry : groupResults.entrySet()) {

			String groupByKey = entry.getKey();
			GroupResult groupResult = entry.getValue();

			String seriesKey;

			if (multiServices) {
				seriesKey = groupByKey + GrafanaFunction.SERVICE_SEPERATOR + groupResult.getServiceId();
			} else {
				seriesKey = groupByKey;
			}

			SeriesResult seriesResult = resultMap.get(seriesKey);

			if (seriesResult == null) {
				Series series = createSeries(request, seriesKey);
				seriesResult = SeriesResult.of(series, groupResult.compareBy);
				resultMap.put(seriesKey, seriesResult);
			}

			for (EventVolume eventVolume : groupResult.volumes) {
				Object value = getEventVolumeValue(request, eventVolume);
				Long time = TimeUtils.getLongTime(eventVolume.time);
				seriesResult.series.values.add(Arrays.asList(new Object[] { time, value }));
			}
		}

		return resultMap.values();
	}

	private List<SeriesResult> process(GroupByInput request, String[] services,
			Collection<Pair<String, String>> timeSpans) {

		List<SeriesResult> result = new ArrayList<SeriesResult>();

		for (String serviceId : services) {
			Map<String, GroupResult> groupResults = processServiceGroupBy(serviceId, request, timeSpans);
			result.addAll(processGroupResults(request, groupResults, services.length > 1));
		}

		return result;

	}

	@Override
	public List<Series> process(FunctionInput functionInput) {
		if (!(functionInput instanceof GroupByInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		GroupByInput request = (GroupByInput) functionInput;

		String[] services = getServiceIds(request);
		Collection<Pair<String, String>> timeSpans = getTimeSpans(request);

		List<SeriesResult> output = process(request, services, timeSpans);

		int limit;

		if (request.limit > 0) {
			limit = Math.min(request.limit, output.size());
			sortOutputByValue(output);

		} else {
			limit = output.size();
			sortOutputByTag(output);
		}

		List<Series> result = new ArrayList<Series>();

		for (int i = 0; i < limit; i++) {
			result.add(output.get(i).series);
		}

		if (request.limit > 0) {
			Collections.reverse(result);
		}

		return result;
	}

	private void sortOutputByValue(List<SeriesResult> output) {
		output.sort(new Comparator<SeriesResult>() {

			@Override
			public int compare(SeriesResult o1, SeriesResult o2) {
				SeriesResult p1 = (SeriesResult) o1;
				SeriesResult p2 = (SeriesResult) o2;

				if ((p1.maxValue == null) || (p2.maxValue == null)) {
					return 0;
				}

				int result = p2.maxValue.compareTo(p1.maxValue);

				return result;
			}
		});
	}

	private void sortOutputByTag(List<SeriesResult> output) {
		output.sort(new Comparator<SeriesResult>() {

			@Override
			public int compare(SeriesResult o1, SeriesResult o2) {
				SeriesResult p1 = (SeriesResult) o1;
				SeriesResult p2 = (SeriesResult) o2;

				if ((p1.series.tags == null) || (p2.series.tags == null)) {
					return 0;
				}

				String tag1 = p1.series.tags.get(0);
				String tag2 = p2.series.tags.get(0);

				int result = tag1.compareTo(tag2);

				return result;
			}
		});
	}
}
