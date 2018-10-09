package com.takipi.integrations.grafana.servlet;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import javax.servlet.http.HttpServletRequest;

public class ServletUtil {
	public static Auth getAuthentication(HttpServletRequest request) {
		String authorization = request.getHeader("Authorization");

		if (authorization != null && authorization.toLowerCase().startsWith("basic")) {
			String base64Credentials = authorization.substring("Basic".length()).trim();
			byte[] credDecoded = Base64.getDecoder().decode(base64Credentials);
			String credentials = new String(credDecoded, StandardCharsets.UTF_8);
			int sep = credentials.lastIndexOf(':');
			
			Auth auth = new Auth();
			auth.hostname = credentials.substring(0, sep);
			auth.token = credentials.substring(sep+1);
			
			return auth;
		}
		
		return null;
	}

	public static class Auth {
		public String hostname;
		public String token;
	}
}
