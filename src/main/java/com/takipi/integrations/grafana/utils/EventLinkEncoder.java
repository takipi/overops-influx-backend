package com.takipi.integrations.grafana.utils;

import java.util.Collection;
import java.util.regex.Pattern;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.event.EventUtil;
import com.takipi.integrations.grafana.input.EventFilterInput;
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

	public static String getSnapshotLink(ApiClient apiClient, String link) {

		if ((link == null) || (link.isEmpty())) {
			throw new IllegalArgumentException("link");
		}

		String[] parts = link.split(Pattern.quote("#"));

		if (parts.length < 4) {
			throw new IllegalArgumentException("Cannot decode " + link);
		}

		String serviceId = parts[0];
		String eventId = String.valueOf(TimeUtils.decodeBase64(parts[1]));

		long fromValue = TimeUtils.decodeBase64(parts[2]);
		long toDelta = TimeUtils.decodeBase64(parts[3]);

		DateTime from = new DateTime(fromValue);
		DateTime to = new DateTime(fromValue + toDelta);

		Collection<String> applications;
		Collection<String> deployments;
		Collection<String> servers;
		
		if (parts.length > 4) {

			if (parts.length < 7) {
				throw new IllegalArgumentException("Cannot decode " + link);
			}

			String appStr = decodeSafe(parts[4]);
			String serverStr = decodeSafe(parts[5]);
			String depStr = decodeSafe(parts[6]);			
			
			EventFilterInput input = new EventFilterInput();
			input.applications = appStr;
			input.servers = serverStr;
			input.deployments = depStr;
			
			applications = input.getServers(serviceId);
			deployments = input.getDeployments(serviceId);
			servers = input.getApplications(serviceId);
		} else {
			applications = null;
			servers = null;
			deployments = null;
		}
				
		String result = EventUtil.getEventRecentLinkDefault(apiClient, serviceId, eventId,
			from, to, applications, servers, deployments, EventUtil.DEFAULT_PERIOD);
		
		return result;

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

		if ((input.hasApplications()) || (input.hasServers()) || (input.hasDeployments())) {
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
