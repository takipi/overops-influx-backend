
package com.takipi.integrations.grafana.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.settings.FolderSettingsStorage;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.util.SettingsUtil;

@WebServlet("/settings")
public class SettingsServlet extends HttpServlet {

	private static final long serialVersionUID = -8423366031016047591L;

	private boolean disabled = false;
	
    @Override
    public void init() throws ServletException
    {
        GrafanaSettings.init(new FolderSettingsStorage());
        
        String disabledStr = ServletUtil.getConfigParam(this, "disabled");

		if (disabledStr != null) {
			disabled = Boolean.parseBoolean(disabledStr);
		}
    }
    
    @Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
    	if (disabled) {
    		response.sendError(HttpServletResponse.SC_NOT_FOUND);
    		return;
    	}
    	
		String data = IOUtils.toString(request.getInputStream(), Charset.forName("UTF-8"));
		
		String resp = SettingsUtil.process(data);
		
		PrintWriter writer = response.getWriter();

		response.setContentType("text/plain");
		response.setCharacterEncoding("UTF-8");
		
		if (resp != null) {
			System.err.println(resp);
			writer.write("Error applying settings: " + resp);
		} else {
			writer.write("Settings saved");
		}

		writer.flush();
	}
}
