
package com.takipi.integrations.grafana.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.takipi.integrations.grafana.util.SettingsUtil;

@WebServlet("/settings")
public class SettingsServlet extends HttpServlet {

	private static final long serialVersionUID = -8423366031016047591L;

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		response.setContentType("text/plain");
		response.setCharacterEncoding("UTF-8");

		PrintWriter writer = response.getWriter();

		SettingsUtil.process(request.getInputStream(), writer);
	}
}
