package com.takipi.integrations.grafana.servlet;

import java.io.IOException;
import java.text.DecimalFormat;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.takipi.integrations.grafana.servlet.ServletUtil.Auth;
import com.takipi.integrations.grafana.util.QueryUtil;

@WebServlet(name="QueryServlet", urlPatterns="/query")
public class QueryServlet extends HttpServlet {
	private static final Logger logger = LoggerFactory.getLogger(QueryServlet.class);

	private static final long serialVersionUID = -8413366001016047591L;

	private static final DecimalFormat df = new DecimalFormat("#.00");

	private boolean logQuery = false;
	private boolean logResponse = false;
	private boolean disabled = false;

	@Override
	public void init() throws ServletException {
		super.init();

		String logQueryStr = ServletUtil.getConfigParam(this, "logQuery");

		if (logQueryStr != null) {
			logQuery = Boolean.parseBoolean(logQueryStr);
		}

		String logResponseStr = ServletUtil.getConfigParam(this, "logResponse");

		if (logResponseStr != null) {
			logResponse = Boolean.parseBoolean(logResponseStr);
		}
		
		String disabledStr = ServletUtil.getConfigParam(this, "disabled");

		if (disabledStr != null) {
			disabled = Boolean.parseBoolean(disabledStr);
		}
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		if (disabled) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		String query = request.getParameter("q");

		Auth auth = ServletUtil.getAuthentication(request);
		
		if (logQuery) {
			logger.debug("OO-AS-INFLUX | Auth: {}", auth);		
			logger.info("OO-AS-INFLUX | Query: {} from {}", query, request.getRemoteAddr());
		}
		
		long t1 = System.currentTimeMillis();
		
		String json = QueryUtil.query(auth, query);

		long t2 = System.currentTimeMillis();
		
		double secs = (double) (t2 - t1) / 1000;

		if (logResponse) {
			logger.info("OO-AS-INFLUX | Query ended {} ", 
					df.format(secs) + " secs: " + json.substring(0, Math.min(1000, json.length())));
		}

		// needed so emoji chars properly encoded when returned by query
		response.setCharacterEncoding("UTF-8");
		
		response.getWriter().append(json);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}
}
