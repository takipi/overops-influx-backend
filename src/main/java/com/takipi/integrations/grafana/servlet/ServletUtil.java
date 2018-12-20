package com.takipi.integrations.grafana.servlet;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

import com.takipi.integrations.grafana.settings.GrafanaSettings;

public class ServletUtil {
	public static String getConfigParam(HttpServlet servlet, String key) {
		return servlet.getServletConfig().getInitParameter(key);
	}
	
	public static Auth getAuthentication(HttpServletRequest request) {
		return getAuthentication(request, null);
	}
	
	// When oo-as-influx is bundled inside OverOps API Server, we want it to pass its address.
	// When oo-as-influx is standalone the authenticated username is the OverOps API Server address.
	//
	public static Auth getAuthentication(HttpServletRequest request, String hostname) {
		// Proxy by OverOps Server
		String hiddenToken = request.getHeader("X-API-KEY");
		
		if ((hiddenToken != null) && (!hiddenToken.isEmpty())) {
			Auth auth = new Auth();
			
			auth.hostname = request.getHeader("X-OO-API");
			auth.token = hiddenToken;
			return auth;
		}
		
		// Datasource authorization
		String authorization = request.getHeader("Authorization");

		if (authorization != null && authorization.toLowerCase().startsWith("basic")) {
			String base64Credentials = authorization.substring("Basic".length()).trim();
			byte[] credDecoded = Base64.getDecoder().decode(base64Credentials);
			String credentials = new String(credDecoded, StandardCharsets.UTF_8);
			int sep = credentials.lastIndexOf(':');
			
			String user = credentials.substring(0, sep);
			
			Auth auth = new Auth();
			
			if (hostname != null)
			{
				auth.hostname = hostname;
			}
			else
			{
				auth.hostname = user;
			}
			
			auth.token = credentials.substring(sep+1);
			
			return auth;
		}
		
		return null;
	}

	public static class Auth {
		public String hostname;
		public String token;
		
		@Override
		public String toString()
		{
			return hostname + ", token size: " + token.length();
		}
	}
}
