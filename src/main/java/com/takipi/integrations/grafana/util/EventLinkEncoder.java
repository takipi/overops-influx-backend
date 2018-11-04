package com.takipi.integrations.grafana.util;

import java.util.Base64;
import java.util.Collection;
import java.util.List;

import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.takipi.api.client.request.event.EventSnapshotRequest;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.input.ViewInput;

public class EventLinkEncoder {
	private static final String TEMPLATE = "{\"service_id\":\"%s\",\"viewport_strings\":{\"from_timestamp\":\"%s\"," +
				"\"to_timestamp\":\"%s\",\"until_now\":false,"+
				"\"machine_names\":[%s],\"agent_names\":[%s],\"deployment_names\":[%s],"+
				"\"request_ids\":[%s]},\"timestamp\":\"%s\"}";

	public static class Link {
		public String link;

		public static Link newLink(String link) {
			Link lnk = new Link();
			lnk.link = link;

			return lnk;
		}
	}
	
	public static Link encodeLink(String serviceId, ViewInput input, EventResult event, DateTime from, DateTime to) {
		EventSnapshotRequest.Builder builder = EventSnapshotRequest.newBuilder().setServiceId(serviceId)
				.setFrom(from.toString()).setTo(to.toString()).setEventId(event.id);

		GrafanaFunction.applyFilters(input, serviceId, builder);
		EventSnapshotRequest request = builder.build();

		String json = String.format(TEMPLATE, serviceId, toEpoch(from), toEpoch(to),
				toList(request.servers), toList(request.apps), toList(request.deployments),
				event.id, toEpoch(to));

		String encoded = Base64.getUrlEncoder().encodeToString(json.getBytes());

		return Link.newLink(encoded);
	}

	private static String toEpoch(DateTime date) {
		return String.valueOf(date.getMillis());
	}

	private static String toList(Collection<String> col) {
		List<String> lst = Lists.newArrayList(col);

		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < lst.size(); i++) {
			String s = lst.get(i);

			sb.append('"');
			sb.append(s);
			sb.append('"');

			if (i < lst.size() - 1) {
				sb.append(',');
			}
		}

		return sb.toString();
	}
}
