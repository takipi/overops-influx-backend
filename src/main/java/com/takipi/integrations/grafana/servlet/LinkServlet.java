package com.takipi.integrations.grafana.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.takipi.integrations.grafana.utils.GrafanaApiClient;
import com.takipi.integrations.grafana.servlet.ServletUtil.Auth;
import com.takipi.integrations.grafana.utils.EventLinkEncoder;

@WebServlet("/link")
public class LinkServlet extends HttpServlet {
	private static final long serialVersionUID = -8920293129859196383L;

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Auth auth = ServletUtil.getAuthentication(request);
		
		String encoded = request.getParameter("link");
		String url = EventLinkEncoder.getSnapshotLink(GrafanaApiClient.getApiClient(auth.hostname, auth.token), encoded);
		
		StringBuilder builder = new StringBuilder();
		builder.append("<!DOCTYPE html>\\\n<html>\n<head>\n<!-- HTML meta refresh URL redirection -->\n<meta http-equiv=\"refresh\"");
		builder.append("content=\"0; url=");
		builder.append(url);
		builder.append("\">\n</head>\n<body></body>\n</html>\";\"");
		
		String output = builder.toString();
		
		response.getWriter().append(output);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
