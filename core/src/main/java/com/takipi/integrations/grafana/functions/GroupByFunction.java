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
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.event.Stats;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseVolumeInput.AggregationType;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.GroupByInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings;
import com.takipi.integrations.grafana.settings.GroupSettings.Group;
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
				
				EventFilter eventFilter = getEventFilter(serviceId, input, timeSpan);

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
	
		protected GroupByFilterAsyncTask(Map<GroupByKey, GroupByVolume> map, String key, GroupByInput input,
				String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan) {

			super(map);
			this.groupKey = key;
			this.input = input;
			this.serviceId = serviceId;
			this.viewId = viewId;
			this.timeSpan = timeSpan;
		}

		@Override
		public String toString() {
			return String.join(" = ", "GroupBy Interval", serviceId, viewId, timeSpan.toString(), groupKey);
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
					executeFilteredVolume(map, groupKey, input, serviceId, timeSpan);
				} else {
					executeFilteredGraph(map, input, serviceId, viewId, timeSpan, intervals,
							groupKey);
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
			
			if (event.error_location != null) {
				updateMap(map, request, event.error_location.prettified_name, time, value);
			}
			break;

		case entryPoint:
			
			if (event.entry_point != null) {
				updateMap(map, request, event.entry_point.prettified_name, time, value);
			}
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
			throw new IllegalStateException("Unsupported event grouping " + request.field);
		}
	}

	private List<BaseGroupByAsyncTask> processEventsGroupBy(Map<GroupByKey, GroupByVolume> map, GroupByInput input,
			String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan) {

		return Collections.singletonList(new GroupByEventAsyncTask(map, serviceId, input, viewId, timeSpan));
	}

	private List<BaseGroupByAsyncTask> processApplicationsGroupBy(Map<GroupByKey, GroupByVolume> map,
			GroupByInput input, String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan) {

		List<BaseGroupByAsyncTask> result = new ArrayList<BaseGroupByAsyncTask>();

		List<String> apps;

		if (input.hasApplications()) {
			apps = new ArrayList<String>(input.getApplications(apiClient, serviceId));
		} else {
			
			List<String> keyApps = new ArrayList<String>();
			
			GroupSettings appGroups = GrafanaSettings.getData(apiClient, serviceId).applications;
			
			if ((appGroups != null) && (appGroups.groups != null)) {
				
				for (Group appGroup : appGroups.groups) {
					keyApps.add(appGroup.toGroupName());
				}
			} 
			
			List<String> liveApps = new ArrayList<String>(ClientUtil.getApplications(apiClient, serviceId, true));

			liveApps.sort(new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					return o1.compareTo(o2);
				}	
			});
			
			apps = new ArrayList<String>();
			
			apps.addAll(keyApps);
			apps.addAll(liveApps);			
		}
		
		List<String> applications = new ArrayList<String>(apps);

		int size;
		
		if (input.limit > 0) {
			size = Math.min(applications.size(), input.limit);
		} else {
			size = applications.size();
		}
		
		String json = new Gson().toJson(input);
		
		for (int i = 0; i < size; i++) {

			String application = applications.get(i);
			
			GroupByInput appInput = new Gson().fromJson(json, input.getClass());
			appInput.applications = application;
			
			result.add(new GroupByFilterAsyncTask(map, GroupSettings.fromGroupName(application),
				appInput, serviceId, viewId, timeSpan));

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

		String json = new Gson().toJson(input);

		for (int i = 0; i < size; i++) {

			String server = servers.get(i);
			
			GroupByInput serverInput = new Gson().fromJson(json, input.getClass());
			serverInput.applications = server;
			
			result.add(new GroupByFilterAsyncTask(map, server, serverInput, serviceId, viewId, timeSpan));
		}

		return result;
	}

	private void executeFilteredGraph(Map<GroupByKey, GroupByVolume> map, GroupByInput input,
			String serviceId, String viewId, Pair<DateTime, DateTime> timespan,
			List<Pair<DateTime, DateTime>> intervals, String key) {
		
		Graph graph = getEventsGraph(serviceId, viewId, input.pointsWanted, input, input.volumeType, timespan.getFirst(), timespan.getSecond());

		if (graph == null) {
			return;
		}
		
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timespan.getFirst(),
				timespan.getSecond(), input.volumeType, input.pointsWanted);

		if (eventsMap == null) {
			return;
		}
			
		EventFilter eventFilter = getEventFilter(serviceId, input, timespan);

		if (eventFilter == null) {
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

				long value;

				if (input.volumeType.equals(VolumeType.invocations)) {
					value = gpc.stats.invocations;
				} else {
					value = gpc.stats.hits;
				}
				
				updateMap(map, input, key, interval.getFirst(), value);				
			}
		}
	}

	private void executeFilteredVolume(Map<GroupByKey, GroupByVolume> map, String key, GroupByInput input,
		String serviceId, Pair<DateTime, DateTime> timespan) {


		Map<String, EventResult> events = getEventMap(serviceId, input, timespan.getFirst(), timespan.getSecond(), 
			input.volumeType, input.pointsWanted);
		
		if (events != null) {
			updateMap(map, serviceId, key, timespan, events.values(), input);
		}
	}

	private List<BaseGroupByAsyncTask> processDeploymentsGroupBy(Map<GroupByKey, GroupByVolume> map,
			GroupByInput input, String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan) {

		List<BaseGroupByAsyncTask> result = new ArrayList<BaseGroupByAsyncTask>();

		List<String> deployments;

		if (input.hasDeployments()) {
			deployments = input.getDeployments(serviceId);
		} else {
			List<String> activeDeps =  new ArrayList<String>(
				ClientUtil.getDeployments(apiClient, serviceId, true));
			
			DeploymentUtil.sortDeployments(activeDeps);
			
			if ((input.limit > 0) && (activeDeps.size() < input.limit)) {
				
				List<String> allDeps =  new ArrayList<String>(
						ClientUtil.getDeployments(apiClient, serviceId, false));			
			
				DeploymentUtil.sortDeployments(allDeps);
								
				for (String dep : allDeps) {
					if (!activeDeps.contains(dep)) {
						activeDeps.add(dep);						
					}
					
					if (activeDeps.size() >= input.limit) {
						break;
					}
				}				
			}
			
			deployments = activeDeps;
		}
				
		int size;
		
		if (input.limit > 0) {
			size = Math.min(deployments.size(), input.limit);
		} else {
			size = deployments.size();
		}
		
		String json = new Gson().toJson(input);

		for (int i = 0; i < size; i++) {

			String deployment = deployments.get(i);
			
			GroupByInput depInput = new Gson().fromJson(json, input.getClass());
			depInput.deployments = deployment;
			
			result.add(new GroupByFilterAsyncTask(map, deployment, depInput, serviceId, viewId, timeSpan));
		}

		return result;
	}

	private void updateMap(Map<GroupByKey, GroupByVolume> map, String serviceId, String key, 
			Pair<DateTime, DateTime> timespan,
			Collection<EventResult> events, GroupByInput input) {

		if (events == null) {
			return;
		}

		EventFilter eventFilter = getEventFilter(serviceId, input, timespan);
		
		if (eventFilter == null) {
			return;
		}
		
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

			updateMap(map, input, key, timespan.getFirst(), value);
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

		int interval = TimeUtil.parseInterval(input.interval);
		long milliInterval = TimeUnit.MINUTES.toMillis(interval);

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

	private static Map<String, SeriesResult> processGroupResults(GroupByInput input,
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

		return resultMap;
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

	private Map<String, SeriesResult> process(GroupByInput input, Collection<String> services, Pair<DateTime, DateTime> timeSpan) {

		Map<String, SeriesResult> result = new HashMap<String, SeriesResult>();

		for (String serviceId : services) {
			Map<String, GroupResult> groupResults = processServiceGroupBy(serviceId, input, timeSpan);
			result.putAll(processGroupResults(input, groupResults, services));
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
		Map<String, SeriesResult> seriesMap = process(input, serviceIds, timespan);
		
		List<Map.Entry<String, SeriesResult>> sorted = new ArrayList<Map.Entry<String, SeriesResult>>(seriesMap.entrySet());
		
		int limit;

		switch (input.field) {
			
			case deployment:
				sortSeries(sorted, true);
				break;
				
			case server:
			case application:
				sortSeries(sorted, false);
				break;
			
			default:
				sortOutputByValue(sorted);
				
		}
		 
		if (input.limit > 0) {
			limit = Math.min(input.limit, sorted.size());
		} else {
			limit = sorted.size();
		}

		List<Series> result = new ArrayList<Series>();

		for (int i = 0; i < limit; i++) {
			result.add(sorted.get(i).getValue().series);
		}

		return result;
	}

	private void sortOutputByValue(List<Map.Entry<String, SeriesResult>> output) {
		
		output.sort(new Comparator<Map.Entry<String, SeriesResult>>() {

			@Override
			public int compare(Map.Entry<String, SeriesResult> p1, Map.Entry<String, SeriesResult> p2) {
				
				if ((p1.getValue().maxValue == null) || (p2.getValue().maxValue == null)) {
					return 0;
				}

				int result = p2.getValue().maxValue.compareTo(p1.getValue().maxValue);

				return result;
			}
		});
	}

	private void sortSeries(List<Map.Entry<String, SeriesResult>> output, boolean byDep) {
		
		output.sort(new Comparator<Map.Entry<String, SeriesResult>>() {
			
			@Override
			public int compare(Map.Entry<String, SeriesResult> p1, Map.Entry<String, SeriesResult> p2) {

				int result;
				
				if (byDep) {
					result = DeploymentUtil.compareDeployments(p2.getKey(), p1.getKey());
				} else {
					result = p1.getKey().compareTo(p2.getKey());
				}

				return result;
			}
		});
	}
}
