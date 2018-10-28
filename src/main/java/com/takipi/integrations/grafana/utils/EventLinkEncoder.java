package com.takipi.integrations.grafana.utils;

import java.util.regex.Pattern;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.event.EventSnapshotRequest;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.result.event.EventSnapshotResult;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.input.FilterInput;
import com.takipi.integrations.grafana.input.ViewInput;

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

		String from = TimeUtils.getDateTimeFromEpoch(fromValue);
		String to = TimeUtils.getDateTimeFromEpoch(fromValue + toDelta);
		
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
		}
		
		DateTime now = DateTime.now();
		
		builder.setFrom(TimeUtils.toString(now.minusMonths(1)));
		builder.setTo(TimeUtils.toString(now));

		response = apiClient.get(builder.build());

		if ((response.isOK()) && (response.data != null)) {
			return response.data.link;
		}
		
		throw new IllegalStateException("Could not provide link for " + value);

	}

	public static String encodeLink(String serviceId, ViewInput input, EventResult event, DateTime from, DateTime to) {

		StringBuilder builder = new StringBuilder();

		builder.append(serviceId);
		builder.append("#");
		builder.append(TimeUtils.encodeBase64(Long.valueOf(event.id)));
		builder.append("#");
		builder.append(TimeUtils.encodeBase64(from.getMillis()));
		builder.append("#");
		builder.append(TimeUtils.encodeBase64(TimeUtils.getDateTimeDelta(from, to)));
		
		if ((input.hasApplications())|| (input.hasServers()) || (input.hasDeployments())) {
			builder.append("#");
			builder.append(encodeSafe(input.applications));
			builder.append("#");
			builder.append(encodeSafe(input.servers));
			builder.append("#");
			builder.append(encodeSafe(input.deployments));
		}

		String result = builder.toString();
		
		return result;
	}
	
}
