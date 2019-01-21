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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.event.Stats;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.request.event.EventsVolumeRequest;
import com.takipi.api.client.request.metrics.GraphRequest;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.result.event.EventsVolumeResult;
import com.takipi.api.client.result.metrics.GraphResult;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.api.client.util.validation.ValidationUtil.GraphType;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseVolumeInput.AggregationType;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GroupByInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.DeploymentUtil;
import com.takipi.integrations.grafana.util.TimeUtil;

public class GroupByFunction extends BaseVolumeFunction {

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

	protected static class GroupByKey {
		
		protected String key;
		protected DateTime time;

		public static GroupByKey of(String key, DateTime time) {
			GroupByKey result = new GroupByKey();
			result.key = key;
			result.time = time;

			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return ((key == null) &&
						(time == null));
			}

			GroupByKey other = (GroupByKey) obj;

			if (!key.equals(other.key)) {
				return false;
			}

			if (!time.equals(other.time)) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			return key.hashCode();
		}
	}

	protected static class GroupByVolume {
		
		protected long sum;
		protected long count;
		protected Comparable<Object> compareBy;
		protected DateTime time;

		public static GroupByVolume of(Comparable<Object> compareBy, DateTime time) {
			GroupByVolume result = new GroupByVolume();
			result.compareBy = compareBy;
			result.time = time;

			return result;
		}
	}

	protected static class GroupByValue {
		
		protected Object sum;
		protected Object avg;
		protected Object count;
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
		
		private List<GroupByVolume> volumes;
		private String serviceId;
		private Comparable<Object> compareBy;

		public GroupResult(String serviceId) {
			this.serviceId = serviceId;
			this.volumes = new ArrayList<GroupByVolume>();
		}

		public void addVolume(GroupByVolume volume) {
			volumes.add(volume);
		}

		public Collection<GroupByVolume> getVolumes() {
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

	protected class BaseGroupByAsyncTask extends BaseAsyncTask implements Callable<Object> {
		public Map<GroupByKey, GroupByVolume> map;

		protected BaseGroupByAsyncTask(Map<GroupByKey, GroupByVolume> map) {
			this.map = map;
		}

		@Override
		public Object call() throws Exception {
			return null;
		}
	}

	protected class AsyncResult {
	}

	protected class GroupByEventAsyncTask extends BaseGroupByAsyncTask {

		protected String serviceId;
		protected GroupByInput input;
		protected Pair<DateTime, DateTime> timeSpan;
		protected String viewId;

		protected GroupByEventAsyncTask(Map<GroupByKey, GroupByVolume> map, String serviceId, GroupByInput input,
				String viewId, Pair<DateTime, DateTime> timeSpan) {
			super(map);
			this.serviceId = serviceId;
			this.input = input;
			this.timeSpan = timeSpan;
			this.viewId = viewId;
		}

		@Override
		public String toString() {
			return String.join(" ", "Event GroupBy", serviceId, timeSpan.toString(), viewId);
		}

		private void executeEventsGraph(Map<String, EventResult> eventsMap, EventFilter eventFilter,
				List<Pair<DateTime, DateTime>> intervals) {

			Graph graph = getEventsGraph(serviceId, viewId, intervals.size() * 5, input, input.volumeType,
					timeSpan.getFirst(), timeSpan.getSecond());
			
			if (graph == null) {
				return;
			}

			for (GraphPoint gp : graph.points) {

				if (gp.contributors == null) {
					continue;
				}

				for (GraphPointContributor gpc : gp.contributors) {

					EventResult event = eventsMap.get(gpc.id);

					if (event == null) {
						continue;
					}

					if (eventFilter.filter(event)) {
						continue;
					}

					int index = TimeUtil.getStartDateTimeIndex(intervals, gp.time);

					if (index == -1) {
						continue;
					}
					
					Pair<DateTime, DateTime> interval = intervals.get(index);

					processEventGroupBy(input, map, event, eventFilter, gpc.stats, interval.getFirst());
				}
			}
		}

		private void executeEventsVolume(Map<String, EventResult> eventsMap, EventFilter eventFilter,
				Pair<DateTime, DateTime> timespan) {

			for (EventResult event : eventsMap.values()) {

				if (eventFilter.filter(event)) {
					continue;
				}

				processEventGroupBy(input, map, event, eventFilter, event.stats, timespan.getFirst());
			}

		}

		@Override
		public AsyncResult call() throws Exception {

			beforeCall();

			try {

				Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timeSpan.getFirst(),
						timeSpan.getSecond(), input.volumeType, input.pointsWanted);

				if (eventsMap == null) {
					return null;
				}
				
				EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);

				List<Pair<DateTime, DateTime>> intervals = getTimeSpans(input);

				if (intervals.size() == 0) {
					return null;
				}

				if (intervals.size() > 1) {
					executeEventsGraph(eventsMap, eventFilter, intervals);
				} else {
					executeEventsVolume(eventsMap, eventFilter, intervals.get(0));
				}

				return null;
			} finally {
				afterCall();
			}
		}
	}

	protected class GroupByFilterAsyncTask extends BaseGroupByAsyncTask {

		protected String groupKey;
		protected GroupByInput input;
		protected String serviceId;
		protected String viewId;
		protected Pair<DateTime, DateTime> timeSpan;
		protected Collection<String> applications;
		protected Collection<String> servers;
		protected Collection<String> deployments;

		protected GroupByFilterAsyncTask(Map<GroupByKey, GroupByVolume> map, String key, GroupByInput input,
				String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan, Collection<String> applications,
				Collection<String> servers, Collection<String> deployments) {

			super(map);
			this.groupKey = key;
			this.input = input;
			this.serviceId = serviceId;
			this.viewId = viewId;
			this.timeSpan = timeSpan;
			this.applications = applications;
			this.servers = servers;
			this.deployments = deployments;
		}

		private String toArray(Collection<String> s) {
			if (s == null) {
				return "";
			}

			return Arrays.toString(s.toArray());
		}

		@Override
		public String toString() {
			return String.join(" = ", "GroupBy Interval", serviceId, viewId, timeSpan.toString(), toArray(applications),
					toArray(servers), toArray(deployments));
		}

		@Override
		public AsyncResult call() throws Exception {

			beforeCall();

			try {
				List<Pair<DateTime, DateTime>> intervals = getTimeSpans(input);

				if (intervals.size() == 0) {
					return null;
				}

				if (intervals.size() == 1) {
					executeFilteredVolume(map, groupKey, input, serviceId, viewId, timeSpan, applications, servers,
							deployments);
				} else {
					executeFilteredGraph(map, input, serviceId, viewId, timeSpan, intervals, applications,
							servers, deployments);
				}

				return null;
			} finally {
				afterCall();
			}
		}
	}

	public GroupByFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private static void updateMap(Map<GroupByKey, GroupByVolume> map, GroupByInput input, String key, DateTime time,
			long value) {
		updateMap(map, input, key, time, value, null);
	}

	@SuppressWarnings("unused")
	private static void updateMap(Map<GroupByKey, GroupByVolume> map, GroupByInput input, String key, DateTime time,
			long value, Comparable<Object> compareBy) {

		if (key == null) {
			return;
		}

		synchronized (map) {

			GroupByKey groupByKey = GroupByKey.of(key, time);
			GroupByVolume groupByVolume = map.get(groupByKey);

			if (groupByVolume == null) {
				groupByVolume = GroupByVolume.of(compareBy, time);
				map.put(groupByKey, groupByVolume);
			}

			groupByVolume.sum = groupByVolume.sum + value;
			groupByVolume.count = groupByVolume.count + 1;

			if (groupByVolume.compareBy != null) {
				int compareResult = groupByVolume.compareBy.compareTo(compareBy);

				if (compareResult > 0) {
					groupByVolume.compareBy = compareBy;
				}
			} else {
				groupByVolume.compareBy = compareBy;
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void processEventGroupBy(GroupByInput request, Map<GroupByKey, GroupByVolume> map, EventResult event,
			EventFilter filter, Stats stats, DateTime time) {

		long value;

		if (request.volumeType.equals(VolumeType.invocations)) {
			value = stats.invocations;
		} else {
			value = stats.hits;
		}

		switch (request.field) {
		case type:
			updateMap(map, request, event.type, time, value);
			break;

		case name:
			updateMap(map, request, event.name, time, value);
			break;

		case location:
			updateMap(map, request, event.error_location.prettified_name, time, value);
			break;

		case entryPoint:
			updateMap(map, request, event.entry_point.prettified_name, time, value);
			break;

		case label:
			if (event.labels == null) {
				return;
			}

			for (String label : event.labels) {

				if (filter.labelMatches(label)) {
					updateMap(map, request, label, time, value);
				}
			}

			break;

		case introduced_by:
			Comparable compareBy = TimeUtil.getDateTime(event.first_seen);
			updateMap(map, request, event.introduced_by, time, value, compareBy);
			break;

		default:
			throw new IllegalStateException("Unsupported grouping " + request.field);
		}
	}

	private List<BaseGroupByAsyncTask> processEventsGroupBy(Map<GroupByKey, GroupByVolume> map, GroupByInput input,
			String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan) {

		return Collections.singletonList(new GroupByEventAsyncTask(map, serviceId, input, viewId, timeSpan));
	}

	private List<BaseGroupByAsyncTask> processApplicationsGroupBy(Map<GroupByKey, GroupByVolume> map,
			GroupByInput input, String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan) {

		List<BaseGroupByAsyncTask> result = new ArrayList<BaseGroupByAsyncTask>();

		Collection<String> apps;

		if (input.hasApplications()) {
			apps = input.getApplications(apiClient, serviceId);
		} else {
			apps = ClientUtil.getApplications(apiClient, serviceId);
		}
		
		List<String> applications = new ArrayList<String>(apps);

		int size;
		
		if (input.limit > 0) {
			
			applications.sort(new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					return o1.compareTo(o2);
				}	
			});
			
			size = Math.min(applications.size(), input.limit);
		} else {
			size = applications.size();
		}
		
		for (int i = 0; i < size; i++) {

			String application = applications.get(i);
			
			result.add(new GroupByFilterAsyncTask(map, application, input, serviceId, viewId, timeSpan,
					Collections.singleton(application), input.getServers(serviceId), input.getDeployments(serviceId)));

		}

		return result;
	}

	private List<BaseGroupByAsyncTask> processServersGroupBy(Map<GroupByKey, GroupByVolume> map, GroupByInput input,
			String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan) {

		List<BaseGroupByAsyncTask> result = new ArrayList<BaseGroupByAsyncTask>();

		List<String> servers;

		if (input.hasServers()) {
			servers = input.getServers(serviceId);
		} else {
			servers = ClientUtil.getServers(apiClient, serviceId);
		}
		
		int size;
		
		if (input.limit > 0) {
			
			servers.sort(new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					return o2.compareTo(o1);
				}	
			});
			
			size = Math.min(servers.size(), input.limit);
		} else {
			size = servers.size();
		}

		for (int i = 0; i < size; i++) {

			String server = servers.get(i);
			
			result.add(new GroupByFilterAsyncTask(map, server, input, serviceId, viewId, timeSpan,
					input.getApplications(apiClient, serviceId), Collections.singleton(server), input.getDeployments(serviceId)));
		}

		return result;
	}

	private void executeFilteredGraph(Map<GroupByKey, GroupByVolume> map, GroupByInput input,
			String serviceId, String viewId, Pair<DateTime, DateTime> timespan,
			List<Pair<DateTime, DateTime>> intervals, Collection<String> applications, Collection<String> servers,
			Collection<String> deployments) {

		Pair<String, String> span = TimeUtil.toTimespan(timespan);

		GraphRequest.Builder builder = GraphRequest.newBuilder().setServiceId(serviceId).setViewId(viewId)
				.setGraphType(GraphType.view).setFrom(span.getFirst()).setTo(span.getSecond())
				.setVolumeType(input.volumeType).setWantedPointCount(5);

		applyFilters(input, serviceId, builder);

		for (String app : applications) {
			builder.addApp(app);
		}

		for (String dep : deployments) {
			builder.addDeployment(dep);
		}

		for (String server : servers) {
			builder.addServer(server);
		}

		Response<GraphResult> response = apiClient.get(builder.build());

		if (response.isBadResponse()) {
			throw new IllegalStateException("Could not acquire volume for app / srv/ dep " + applications + "/"
					+ servers + "/" + deployments + " in service " + serviceId + " . Error " + response.responseCode);
		}

		if ((response.data == null) || (response.data.graphs == null) || (response.data.graphs.size() == 0)) {
			return;
		}

		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timespan.getFirst(),
				timespan.getSecond(), input.volumeType, input.pointsWanted);

		EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);

		Graph graph = response.data.graphs.get(0);

		for (GraphPoint gp : graph.points) {

			if (gp.contributors == null) {
				continue;
			}

			for (GraphPointContributor gpc : gp.contributors) {

				EventResult event = eventsMap.get(gpc.id);

				if (event == null) {
					continue;
				}

				if (eventFilter.filter(event)) {
					continue;
				}

				int index = TimeUtil.getStartDateTimeIndex(intervals, gp.time);

				if (index == -1) {
					continue;
				}

				Pair<DateTime, DateTime> interval = intervals.get(index);

				processEventGroupBy(input, map, event, eventFilter, gpc.stats, interval.getFirst());
			}
		}
	}

	private void executeFilteredVolume(Map<GroupByKey, GroupByVolume> map, String key, GroupByInput input,
			String serviceId, String viewId, Pair<DateTime, DateTime> timespan, Collection<String> applications,
			Collection<String> servers, Collection<String> deployments) {

		Pair<String, String> span = TimeUtil.toTimespan(timespan);

		EventsVolumeRequest.Builder builder = EventsVolumeRequest.newBuilder().setServiceId(serviceId)
				.setFrom(span.getFirst()).setTo(span.getSecond()).setViewId(viewId).setVolumeType(input.volumeType);

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

		if (response.isBadResponse()) {
			throw new IllegalStateException("Could not acquire volume for app / srv/ dep " + applications + "/"
					+ servers + "/" + deployments + " in service " + serviceId + " . Error " + response.responseCode);
		}

		if (response.data != null) {
			updateMap(map, serviceId, key, timespan.getFirst(), response.data.events, input);
		}
	}

	private List<BaseGroupByAsyncTask> processDeploymentsGroupBy(Map<GroupByKey, GroupByVolume> map,
			GroupByInput input, String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan) {

		List<BaseGroupByAsyncTask> result = new ArrayList<BaseGroupByAsyncTask>();

		List<String> deployments;

		if (input.hasDeployments()) {
			deployments = input.getDeployments(serviceId);
		} else {
			Pair<String, List<String>> depResult = DeploymentUtil.getActiveDeployment(apiClient, input,
				serviceId, input.limit);
			
			deployments = new ArrayList<String>(depResult.getSecond().size() + 1);
			
			deployments.add(depResult.getFirst());
			deployments.addAll(depResult.getSecond());
		}
			
		int size;
		
		if (input.limit > 0) {
			DeploymentUtil.sortDeployments(deployments);
			size = Math.min(deployments.size(), input.limit);
		} else {
			size = deployments.size();
		}

		for (int i = 0; i < size; i++) {

			String deployment = deployments.get(i);
			
			result.add(new GroupByFilterAsyncTask(map, deployment, input, serviceId, viewId, timeSpan,
					input.getApplications(apiClient, serviceId), input.getServers(serviceId),
					Collections.singleton(deployment)));
		}

		return result;
	}

	private void updateMap(Map<GroupByKey, GroupByVolume> map, String serviceId, String key, DateTime time,
			List<EventResult> events, GroupByInput input) {

		if (events == null) {
			return;
		}

		EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);
		Pattern pattern = input.getPatternFilter();

		for (EventResult event : events) {

			if (eventFilter.filter(event)) {
				continue;
			}

			long value;

			if (input.volumeType.equals(VolumeType.invocations)) {
				value = event.stats.invocations;
			} else {
				value = event.stats.hits;
			}

			if (pattern != null) {
				Matcher match = pattern.matcher(key);

				if ((match != null) && (!match.find())) {
					continue;
				}
			}

			updateMap(map, input, key, time, value);
		}
	}

	private void updateGroupResutMap(Map<String, GroupResult> map, String serviceId, GroupByKey groupByKey,
			GroupByVolume eventVolume) {

		GroupResult groupResult = map.get(groupByKey.key);

		if (groupResult == null) {
			groupResult = new GroupResult(serviceId);
			map.put(groupByKey.key, groupResult);
		}

		groupResult.updateCompareBy(eventVolume.compareBy);
		groupResult.addVolume(eventVolume);
	}

	private Map<String, GroupResult> processServiceGroupBy(String serviceId, GroupByInput input,
			Pair<DateTime, DateTime> timespan) {

		Map<GroupByKey, GroupByVolume> outputMap = new HashMap<GroupByKey, GroupByVolume>();

		List<BaseGroupByAsyncTask> serviceTasks = processServiceGroupBy(outputMap, serviceId, input, timespan);

		if (serviceTasks.size() == 1) {
			try {
				serviceTasks.get(0).call();
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		} else {
			
			int taskCount;
			
			if (input.limit > 0) {
				taskCount = Math.min(input.limit, serviceTasks.size());
			} else {
				taskCount = serviceTasks.size();
			}
			
			Collection<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
			
			for (int i = 0; i < taskCount; i++) {
				BaseGroupByAsyncTask task = serviceTasks.get(i);
				tasks.add(task);
			}
			
			executeTasks(tasks, true);
		}

		Map<String, GroupResult> result = new HashMap<String, GroupResult>();

		for (Map.Entry<GroupByKey, GroupByVolume> entry : outputMap.entrySet()) {

			GroupByKey groupByKey = entry.getKey();
			GroupByVolume eventVolume = entry.getValue();

			updateGroupResutMap(result, serviceId, groupByKey, eventVolume);
		}

		return result;
	}

	private List<BaseGroupByAsyncTask> processServiceGroupBy(Map<GroupByKey, GroupByVolume> map, String serviceId,
			GroupByInput input, Pair<DateTime, DateTime> timeSpan) {

		String viewId = getViewId(serviceId, input.view);

		if (viewId == null) {
			return Collections.emptyList();
		}

		switch (input.field) {
		case application:
			return processApplicationsGroupBy(map, input, serviceId, viewId, timeSpan);

		case deployment:
			return processDeploymentsGroupBy(map, input, serviceId, viewId, timeSpan);

		case server:
			return processServersGroupBy(map, input, serviceId, viewId, timeSpan);

		default:
			return processEventsGroupBy(map, input, serviceId, viewId, timeSpan);
		}
	}

	private List<Pair<DateTime, DateTime>> getTimeSpans(GroupByInput input) {

		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);

		if ((input.interval == null) || (input.interval.length() == 0)) {
			return Collections.singletonList(timeSpan);
		}

		long milliInterval = TimeUtil.parseInterval(input.interval) * 1000 * 60;

		List<Pair<DateTime, DateTime>> result = new ArrayList<Pair<DateTime, DateTime>>();

		DateTime startTime = timeSpan.getFirst();
		DateTime endTime = startTime;

		while (startTime.isBefore(timeSpan.getSecond())) {
			endTime = startTime.plus(milliInterval);
			result.add(Pair.of(startTime, endTime));
			startTime = endTime;
		}

		result.add(Pair.of(startTime, startTime.plus(milliInterval)));

		return result;
	}

	private static GroupByValue getGroupByValue(@SuppressWarnings("unused") GroupByInput input, GroupByVolume eventVolume) {

		GroupByValue result = new GroupByValue();

		result.sum = Long.valueOf(eventVolume.sum);
		result.count = Long.valueOf(eventVolume.count);

		if (eventVolume.count > 0) {
			result.avg = Double.valueOf((double) eventVolume.sum / (double) eventVolume.count);
		} else {
			result.avg = Double.valueOf(0);
		}

		return result;
	}

	private static Series createSeries(GroupByInput input, String seriesKey) {
		Series series = new Series();

		if (input.addTags) {
			series.name = SERIES_NAME;
			series.tags = Collections.singletonList(seriesKey);

			Collection<AggregationType> types = getColumnTypes(input);

			series.columns = new ArrayList<String>();
			series.columns.add(TIME_COLUMN);

			for (AggregationType agType : types) {
				series.columns.add(agType.toString());
			}
		} else {
			series.name = EMPTY_NAME;
			series.columns = Arrays.asList(new String[] { TIME_COLUMN, seriesKey });
		}

		series.values = new ArrayList<List<Object>>();

		return series;
	}

	private static Collection<SeriesResult> processGroupResults(GroupByInput input,
			Map<String, GroupResult> groupResults, Collection<String> serviceIds) {

		Map<String, SeriesResult> resultMap = new HashMap<String, SeriesResult>();

		for (Map.Entry<String, GroupResult> entry : groupResults.entrySet()) {

			String groupByKey = entry.getKey();
			GroupResult groupResult = entry.getValue();

			String seriesKey = getServiceValue(groupByKey, groupResult.getServiceId(), serviceIds);

			SeriesResult seriesResult = resultMap.get(seriesKey);

			if (seriesResult == null) {
				Series series = createSeries(input, seriesKey);
				seriesResult = SeriesResult.of(series, groupResult.compareBy);
				resultMap.put(seriesKey, seriesResult);
			}

			groupResult.volumes.sort(new Comparator<GroupByVolume>() {

				@Override
				public int compare(GroupByVolume o1, GroupByVolume o2) {
					return o1.time.compareTo(o2.time);
				}
			});

			for (GroupByVolume eventVolume : groupResult.volumes) {
				GroupByValue value = getGroupByValue(input, eventVolume);
				Long time = Long.valueOf(eventVolume.time.getMillis());
				appendGroupByValues(input, value, time, seriesResult.series);
			}
		}

		return resultMap.values();
	}

	private static Collection<AggregationType> getColumnTypes(GroupByInput input) {

		List<AggregationType> result = Lists.newArrayList();
		String[] types = input.type.split(ARRAY_SEPERATOR);

		for (String type : types) {

			AggregationType agType = AggregationType.valueOf(type.trim());

			if (agType == null) {
				throw new IllegalStateException(type);
			}

			result.add(agType);

		}

		return result;
	}

	private static void appendGroupByValues(GroupByInput input, 
		GroupByValue value, Long time, Series series) {

		String[] types = input.type.split(ARRAY_SEPERATOR);
		Object[] values = new Object[types.length + 1];

		values[0] = getTimeValue(time, input);

		for (int i = 0; i < types.length; i++) {

			Object columnValue;

			String type = types[i];
			AggregationType agType = AggregationType.valueOf(type.trim());

			switch (agType) {
			case sum:
				columnValue = value.sum;
				break;

			case avg:
				columnValue = value.avg;
				break;

			case count:
				columnValue = value.count;
				break;

			default:
				throw new IllegalStateException(agType.toString());
			}

			values[i + 1] = columnValue;
		}

		series.values.add(Arrays.asList(values));
	}

	private List<SeriesResult> process(GroupByInput input, Collection<String> services, Pair<DateTime, DateTime> timeSpan) {

		List<SeriesResult> result = new ArrayList<SeriesResult>();

		for (String serviceId : services) {
			Map<String, GroupResult> groupResults = processServiceGroupBy(serviceId, input, timeSpan);
			result.addAll(processGroupResults(input, groupResults, services));
		}

		return result;

	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof GroupByInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		GroupByInput input = (GroupByInput) functionInput;

		Collection<String> serviceIds = getServiceIds(input);

		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(input.timeFilter);
		List<SeriesResult> output = process(input, serviceIds, timespan);

		int limit;

		if (input.limit > 0) {
			limit = Math.min(input.limit, output.size());
			sortOutputByValue(output);

		} else {
			limit = output.size();
			sortOutputByTag(output);
		}

		List<Series> result = new ArrayList<Series>();

		for (int i = 0; i < limit; i++) {
			result.add(output.get(i).series);
		}

		if (input.limit > 0) {
			Collections.reverse(result);
		}

		return result;
	}

	private void sortOutputByValue(List<SeriesResult> output) {
		output.sort(new Comparator<SeriesResult>() {

			@Override
			public int compare(SeriesResult p1, SeriesResult p2) {
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
			public int compare(SeriesResult p1, SeriesResult p2) {
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
