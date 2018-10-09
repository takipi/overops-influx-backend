package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class GroupByFunction extends GrafanaFunction {

	public enum AggregationType {
		sum, avg;
	}

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

	public GroupByFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private static void updateMap(Map<String, EventVolume> map, String key, long value) {

		if (key == null) {
			return;
		}

		EventVolume stat = map.get(key);

		if (stat == null) {
			stat = new EventVolume();
			map.put(key, stat);
		}

		stat.sum = stat.sum + value;
		stat.count = stat.count + 1;
	}

	private void processEventGroupBy(GroupByInput request, Map<String, EventVolume> map, EventResult event) {

		long value;

		if (request.volumeType.equals(VolumeType.invocations)) {
			value = event.stats.invocations;
		} else {
			value = event.stats.hits;
		}

		switch (request.field) {
		case type:
			updateMap(map, event.type, value);
			break;

		case name:
			updateMap(map, event.name, value);
			break;

		case location:
			updateMap(map, event.error_location.prettified_name, value);
			break;

		case entryPoint:
			updateMap(map, event.entry_point.prettified_name, value);
			break;

		case label:
			if (event.labels == null) {
				return;
			}

			for (String label : event.labels) {
				updateMap(map, label, value);
			}

			break;

		case introduced_by:
			updateMap(map, event.introduced_by, value);
			break;

		default:
			throw new IllegalStateException("Unsupported grouping " + request.field);
		}
	}

	private Map<String, EventVolume> processEventsGroupBy(GroupByInput request, String serviceId,
			Pair<String, String> timeSpan) {

		Map<String, EventVolume> result = new HashMap<String, EventVolume>();

		List<EventResult> events = getEventList(serviceId, request, timeSpan);

		for (EventResult event : events) {
			processEventGroupBy(request, result, event);
		}

		return result;
	}

	private Map<String, EventVolume> processApplicationsGroupBy(GroupByInput request, String serviceId,
			Pair<String, String> timeSpan) {

		Map<String, EventVolume> result = new HashMap<String, EventVolume>();

		String viewId = getViewId(serviceId, request);

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

			appendVolumeRequest(result, application, request, serviceId, viewId, timeSpan,
					Collections.singleton(application), request.getServers(serviceId),
					request.getDeployments(serviceId));

		}

		return result;
	}

	private Map<String, EventVolume> processServersGroupBy(GroupByInput request, String serviceId,
			Pair<String, String> timeSpan) {

		Map<String, EventVolume> result = new HashMap<String, EventVolume>();

		String viewId = getViewId(serviceId, request);

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

			appendVolumeRequest(result, server, request, serviceId, viewId, timeSpan,
					request.getApplications(serviceId), Collections.singleton(server),
					request.getDeployments(serviceId));

		}

		return result;
	}

	private void appendVolumeRequest(Map<String, EventVolume> map, String key, GroupByInput request, String serviceId,
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

		updateMap(map, key, response.data.events, request.volumeType);

	}

	private Map<String, EventVolume> processDeploymentsGroupBy(GroupByInput request, String serviceId,
			Pair<String, String> timeSpan) {

		Map<String, EventVolume> result = new HashMap<String, EventVolume>();

		String viewId = getViewId(serviceId, request);

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

			appendVolumeRequest(result, deployment, request, serviceId, viewId, timeSpan,
					request.getApplications(serviceId), request.getServers(serviceId),
					Collections.singleton(deployment));

		}

		return result;
	}

	private void updateMap(Map<String, EventVolume> map, String key, List<EventResult> events, VolumeType volumeType) {
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

			updateMap(map, key, value);
		}
	}

	private Map<String, EventVolume> processServiceGroupBy(String serviceId, GroupByInput request,
			Pair<String, String> timeSpan) {

		switch (request.field) {
		case application:
			return processApplicationsGroupBy(request, serviceId, timeSpan);

		case deployment:
			return processDeploymentsGroupBy(request, serviceId, timeSpan);

		case server:
			return processServersGroupBy(request, serviceId, timeSpan);

		default:
			return processEventsGroupBy(request, serviceId, timeSpan);
		}
	}

	@Override
	public QueryResult process(FunctionInput functionInput) {
		if (!(functionInput instanceof GroupByInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		GroupByInput request = (GroupByInput) functionInput;

		Pair<String, String> timeSpan = TimeUtils.parseTimeFilter(request.timeFilter);

		String[] services = getServiceIds(request);

		List<Series> result = new ArrayList<Series>();

		Long time = Long.valueOf(TimeUtils.getLongTime(timeSpan.getSecond()));

		for (String serviceId : services) {
			Map<String, EventVolume> map = processServiceGroupBy(serviceId, request, timeSpan);

			for (Map.Entry<String, EventVolume> entry : map.entrySet()) {
				Series series = new Series();

				series.name = SERIES_NAME;
				series.columns = Arrays.asList(new String[] { TIME_COLUMN, SUM_COLUMN });

				Object value;

				if (request.type.equals(AggregationType.sum)) {
					value = Long.valueOf(entry.getValue().sum);
				} else {
					value = Double.valueOf(entry.getValue().sum / entry.getValue().count);
				}

				series.values = Collections.singletonList(Arrays.asList(new Object[] { time, value }));

				if (services.length == 1) {
					series.tags = Collections.singletonList(entry.getKey());
				} else {
					series.tags = Collections
							.singletonList(entry.getKey() + GrafanaFunction.SERVICE_SEPERATOR + serviceId);
				}

				result.add(series);
			}
		}

		return createQueryResults(result);
	}
}
