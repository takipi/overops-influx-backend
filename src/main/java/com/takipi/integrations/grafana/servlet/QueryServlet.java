package com.takipi.integrations.grafana.servlet;

import java.io.IOException;
import java.text.DecimalFormat;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;
import com.takipi.integrations.grafana.functions.FunctionParser;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.servlet.ServletUtil.Auth;
import com.takipi.integrations.grafana.utils.GrafanaApiClient;

@WebServlet("/query")
public class QueryServlet extends HttpServlet {
	private static final long serialVersionUID = -8413366001016047591L;
	
	private static final DecimalFormat df = new DecimalFormat("#.00"); 
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String query = request.getParameter("q");
		
		if (query.startsWith("SHOW RETENTION POLICIES"))
		{
			response.getWriter().append("{\"results\":[{\"statement_id\":0,\"series\":[{\"columns\":[\"name\",\"duration\",\"shardGroupDuration\",\"replicaN\",\"default\"],\"values\":[[\"autogen\",\"0s\",\"168h0m0s\",1,true]]}]}]}");
			return;
		}
		else if (query.startsWith("SHOW DATABASES"))
		{
			response.getWriter().append("{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"_internal\"],[\"telegraf\"]]}]}]}");
			return;
		}
		
		Auth auth = ServletUtil.getAuthentication(request);
		
		long t1 = System.currentTimeMillis();
		Object output = executeQuery(query, auth);
		String json = new Gson().toJson(output);
		long t2 = System.currentTimeMillis();;	
		
		double secs = (double)(t2 - t1) / 1000;
		System.out.println(query);
		System.out.println(df.format(secs) + " secs: " + json + "\n");
		response.getWriter().append(json);
	}
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}

	private static QueryResult executeQuery(String query, Auth auth) {
		return FunctionParser.processQuery(GrafanaApiClient.getApiClient(auth), query);
	}
}
