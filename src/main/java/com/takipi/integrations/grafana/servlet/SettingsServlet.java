
package com.takipi.integrations.grafana.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.util.GrafanaApiClient;

@WebServlet("/settings")
public class SettingsServlet extends HttpServlet {

	private static final long serialVersionUID = -8423366031016047591L;

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

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		response.setContentType("text/plain");
		response.setCharacterEncoding("UTF-8");

		String error = null;

		PrintWriter writer = response.getWriter();

		try {

			String data = IOUtils.toString(request.getInputStream(), Charset.forName("UTF-8"));
			
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
