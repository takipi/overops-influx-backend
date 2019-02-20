package com.takipi.integrations.grafana.util;

import java.util.Base64;
import java.util.Collection;
import java.util.List;

import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.integrations.grafana.input.ViewInput;

public class EventLinkEncoder {
	
	/*
	private static final String SAAS_API = "api.overops.com";
	private static final String SAAS_APP = "app.overops.com";
	
	private static final String HTTP = "http://";
	private static final String HTTPS = "https://";
	
	private static final String URL = "%s/tale.html?snapshot=%s";
	
	*/

	private static final String TEMPLATE = "{\"service_id\":\"%s\",\"viewport_strings\":{\"from_timestamp\":\"%s\"," +
				"\"to_timestamp\":\"%s\",\"until_now\":false,"+
				"\"machine_names\":[%s],\"agent_names\":[%s],\"deployment_names\":[%s],"+
				"\"request_ids\":[%s]},\"timestamp\":\"%s\"}";
	

	public static String encodeLink(ApiClient apiClient, String serviceId, ViewInput input, EventResult event, DateTime from, DateTime to) {
			
		Collection<String> apps = input.getApplications(apiClient, serviceId);
		Collection<String> deployments = input.getDeployments(serviceId);
		Collection<String> servers = input.getServers(serviceId);

		String json = String.format(TEMPLATE, serviceId, TimeUtil.getMillisAsString(from), TimeUtil.getMillisAsString(to),
				toList(servers), toList(apps), toList(deployments),
				event.id, TimeUtil.getMillisAsString(to));

		String snapshot = Base64.getUrlEncoder().encodeToString(json.getBytes());
		
		//String appUrl = apiClient.getHostname().replace(SAAS_API, SAAS_APP);
		//String result = String.format(URL, appUrl, snapshot);

		return snapshot;
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
