package com.takipi.integrations.grafana.util;

import java.util.Collection;
import java.util.regex.Pattern;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.settings.GrafanaSettings;

public class SettingsUtil {

	private static final char SEPERATOR = '=';

	private static String getSettingsJson(String settings) {

		String escaped = settings.replace("\n", "").replace("\\n", "").replace("\\r", "")
				.replaceAll(Pattern.quote("\\"), "");

		int startIndex = escaped.indexOf('{');
		int endIndex = escaped.lastIndexOf('}');

		String result = escaped.substring(startIndex, endIndex + 1);

		return result;
	}

	private static String getServiceId(String environments) {

		EnvironmentsInput input = new EnvironmentsInput();
		input.environments = environments;

		Collection<String> serviceIds = input.getServiceIds();

		if ((serviceIds == null) || (serviceIds.size() != 1)) {
			throw new IllegalStateException("Invalid env: " + environments);
		}

		return serviceIds.iterator().next();
	}

	public static class SettingsPair
	{
		public String serviceId;
		public String settings;
		
		public static SettingsPair of(String serviceId, String settings) {
			SettingsPair pair = new SettingsPair();
			pair.serviceId = serviceId;
			pair.settings = settings;
			
			return pair;
		}
	}
	
	public static SettingsPair parse(String data) {
		int index = data.indexOf(SEPERATOR);

		if (index == -1) {
			throw new IllegalStateException("Env not found (" + index + ")");
		}

		String rawSettings = data.substring(index + 1, data.length());
		String rawService = data.substring(0, index);

		String serviceId = getServiceId(rawService);

		if ((serviceId == null) || (serviceId.length() == 0)) {
			throw new IllegalArgumentException("No environment ID provided");
		}

		String settings = getSettingsJson(rawSettings);
		
		return SettingsPair.of(serviceId, settings);
	}
	
	public static String process(String data) {
		String error = null;

		try {
			SettingsPair pair = parse(data);

			ApiClient apiClient = GrafanaApiClient.getApiClient();

			GrafanaSettings.saveServiceSettings(apiClient, pair.serviceId, pair.settings);
		} catch (Exception e) {
			error = e.getMessage();
		}
		
		return error;
	}
}
