package com.takipi.integrations.grafana.utils;

import java.util.regex.Pattern;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.request.event.EventSnapshotRequest;
import com.takipi.common.api.result.event.EventResult;
import com.takipi.common.api.result.event.EventSnapshotResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.common.api.util.Pair;
import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FilterInput;

public class EventLinkEncoder {
	private static String encodeSafe(String value) {
		if ((value == null) || (value.length() == 0)) {
			return "*";
		}

		return value;
	}
	
	private static String decodeSafe(String value) {
		if (value.equals("*")) {
			return "";
		}

		return value;
	}

	public static String getSnapshotLink(ApiClient apiClient, String value) {

		String[] parts = value.split(Pattern.quote("#"));

		if (parts.length < 4) {
			throw new IllegalArgumentException("Cannot decode " + value);
		}

		String serviceId = parts[0];
		String eventId = String.valueOf(TimeUtils.decodeBase64(parts[1]));
		
		long fromValue = TimeUtils.decodeBase64(parts[2]);
		long toDelta = TimeUtils.decodeBase64(parts[3]);

		String from = TimeUtils.getDateTime(fromValue);
		String to = TimeUtils.getDateTime(fromValue + toDelta);
		
		EventSnapshotRequest.Builder builder = EventSnapshotRequest.newBuilder().setFrom(from).setTo(to)
				.setServiceId(serviceId).setEventId(eventId);
		
		if (parts.length > 4) {
		
			if (parts.length < 7) {
				throw new IllegalArgumentException("Cannot decode " + value);
			}
			
			String apps = decodeSafe(parts[4]);
			String servers = decodeSafe(parts[5]);
			String deployments = decodeSafe(parts[6]);
			
			FilterInput request = new FilterInput();
			request.applications = apps;
			request.servers = servers;
			request.deployments = deployments;

			GrafanaFunction.applyFilters(request, serviceId, builder);

		}

		Response<EventSnapshotResult> response = apiClient.get(builder.build());

		if ((response.isOK()) && (response.data != null)) {
			return response.data.link;
		} else {
			return "";
		}

	}

	public static String encodeLink(String serviceId, EventsInput request, EventResult event, Pair<String, String> timeSpan) {

		StringBuilder builder = new StringBuilder();

		builder.append(serviceId);
		builder.append("#");
		builder.append(TimeUtils.encodeBase64(Long.valueOf(event.id)));
		builder.append("#");
		builder.append(TimeUtils.encodeBase64(TimeUtils.getLongTime(timeSpan.getFirst())));
		builder.append("#");
		builder.append(TimeUtils.encodeBase64(TimeUtils.getDateTimeDelta(timeSpan)));
		
		if ((request.hasApplications())|| (request.hasServers()) || (request.hasDeployments())) {
			builder.append("#");
			builder.append(encodeSafe(request.applications));
			builder.append("#");
			builder.append(encodeSafe(request.servers));
			builder.append("#");
			builder.append(encodeSafe(request.deployments));
		}

		String result = builder.toString();
		
		return result;
	}
	
}
