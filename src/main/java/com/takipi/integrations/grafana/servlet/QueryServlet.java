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

import com.google.gson.Gson;
import com.takipi.integrations.grafana.functions.FunctionParser;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.servlet.ServletUtil.Auth;
import com.takipi.integrations.grafana.util.GrafanaApiClient;

@WebServlet(name="QueryServlet", urlPatterns="/query") // May be override by web.xml!
public class QueryServlet extends HttpServlet {
	private static final Logger logger = LoggerFactory.getLogger(QueryServlet.class);

	private static final long serialVersionUID = -8413366001016047591L;

	private static final DecimalFormat df = new DecimalFormat("#.00");

	private boolean logQuery = false;
	private boolean logResponse = false;

	@Override
	public void init() throws ServletException {
		super.init();

		String logQueryStr = getConfigParam("logQuery");

		if (logQueryStr != null) {
			logQuery = Boolean.parseBoolean(logQueryStr);
		}

		String logResponseStr = getConfigParam("logResponse");

		if (logResponseStr != null) {
			logResponse = Boolean.parseBoolean(logResponseStr);
		}
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String query = request.getParameter("q");

		if (query.startsWith("SHOW RETENTION POLICIES")) {
			response.getWriter().append(
					"{\"results\":[{\"statement_id\":0,\"series\":[{\"columns\":[\"name\",\"duration\",\"shardGroupDuration\",\"replicaN\",\"default\"],\"values\":[[\"autogen\",\"0s\",\"168h0m0s\",1,true]]}]}]}");
			return;
		} else if (query.startsWith("SHOW DATABASES")) {
			response.getWriter().append(
					"{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"_internal\"],[\"telegraf\"]]}]}]}");
			return;
		}

		Auth auth = ServletUtil.getAuthentication(request);

		if (logQuery) {
			logger.debug("Query: {}", query);
		}

		long t1 = System.currentTimeMillis();

		Object output = executeQuery(query, auth);

		String json = new Gson().toJson(output);

		long t2 = System.currentTimeMillis();
		
		double secs = (double) (t2 - t1) / 1000;

		if (logResponse) {
			logger.debug("Query ended {}",
					df.format(secs) + " secs: " + json.substring(0, Math.min(1000, json.length())));
		}

		response.getWriter().append(json);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}

	private static QueryResult executeQuery(String query, Auth auth) {
		return FunctionParser.processQuery(GrafanaApiClient.getApiClient(auth), query);
	}

	private String getConfigParam(String key) {
		return getServletConfig().getInitParameter(key);
	}
}
