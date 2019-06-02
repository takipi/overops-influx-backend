package com.takipi.integrations.grafana.util;

import java.text.DecimalFormat;

import com.google.gson.Gson;
import com.takipi.integrations.grafana.functions.FunctionParser;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.servlet.ServletUtil.Auth;

public class QueryUtil {
	public static final DecimalFormat df = new DecimalFormat("#.00");
	
	public static String query(Auth auth, String query) {
		if (query == null) {
			return "No query provided";
		}
		
		if (query.startsWith("SHOW RETENTION POLICIES")) {
			return "{\"results\":[{\"statement_id\":0,\"series\":[{\"columns\":[\"name\",\"duration\",\"shardGroupDuration\",\"replicaN\",\"default\"],\"values\":[[\"autogen\",\"0s\",\"168h0m0s\",1,true]]}]}]}";
		} else if (query.startsWith("SHOW DATABASES")) {
			return"{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"_internal\"],[\"telegraf\"]]}]}]}";
		}
	
		Object output = executeQuery(query, auth);
	
		String json = null;
		
		try {
			json = new Gson().toJson(output);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		
		return json;
	}
	
	private static QueryResult executeQuery(String query, Auth auth) {
		return FunctionParser.processQuery(GrafanaApiClient.getApiClient(auth), query);
	}
}
