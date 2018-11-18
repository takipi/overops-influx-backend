package com.takipi.integrations.grafana.util;

import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

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

	public static void process(InputStream inputStream, PrintWriter writer) {

		String error = null;

		try {

			String data = IOUtils.toString(inputStream, Charset.forName("UTF-8"));

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

			ApiClient apiClient = GrafanaApiClient.getApiClient();

			GrafanaSettings.saveServiceSettings(apiClient, serviceId, settings);
		} catch (Exception e) {
			error = e.getMessage();
		}

		if (error != null) {
			System.err.println(error);
			writer.write("Error applying settings: " + error);
		} else {
			writer.write("Settings saved");
		}

		writer.flush();
	}
}
