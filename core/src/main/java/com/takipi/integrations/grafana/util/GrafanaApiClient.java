package com.takipi.integrations.grafana.util;

import com.google.common.base.Strings;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.RemoteApiClient;
import com.takipi.api.client.observe.LoggingObserver;
import com.takipi.integrations.grafana.cache.CachedApiClient;
import com.takipi.integrations.grafana.servlet.ServletUtil.Auth;

public class GrafanaApiClient
{
	private static final String API_TIMEOUT_PROPERTY	= "api.timeout";
	private static final String API_AUTH_PROPERTY		= "api.auth";
	private static final String API_OBSERVE_PROPERTY	= "api.observe";
	
	private static final int DEFAULT_TIMEOUT		= 120000;
	private static final int MIN_TIMEOUT			= 5000;
	
	private static final boolean DEFAULT_OBSERVE	= false;
	
	public static ApiClient getApiClient()
	{
		return getApiClient(null); 
	}
	
	public static ApiClient getApiClient(Auth auth)
	{
		String host;
		String token;
		
		if (auth != null)
		{
			host = auth.hostname;
			token = auth.token;
		}
		else
		{
			String authProp = System.getProperty(API_AUTH_PROPERTY);
			
			if (authProp == null)
			{
				host = "null";
				token = "null";
			}
			else
			{
				int index = authProp.indexOf('=');
				
				if (index == -1)
				{
					throw new IllegalArgumentException(authProp);
				}
				
				host = authProp.substring(0, index);
				token = authProp.substring(index + 1, authProp.length());
			}
		}
		
		return getApiClient(host, token);
	}
	
	public static ApiClient getApiClient(String hostname, String token)
	{
		RemoteApiClient.Builder builder =
				RemoteApiClient.newBuilder()
		            .setHostname(hostname)
		            .setApiKey(token)
		            .setConnectTimeout(getApiTimeout());
		
		if (shouldObserve())
		{
			builder.addObserver(LoggingObserver.create(false));
		}
		
		return CachedApiClient.create(builder.build());
	}
	
	private static int getApiTimeout()
	{
		try
		{
			String apiTimeoutProp = System.getProperty(API_TIMEOUT_PROPERTY);
			
			if (Strings.isNullOrEmpty(apiTimeoutProp))
			{
				return DEFAULT_TIMEOUT;
			}
			
			int apiTimeout = Integer.parseInt(apiTimeoutProp);
			
			return Math.max(MIN_TIMEOUT, apiTimeout);
		}
		catch (Exception e)
		{
			return DEFAULT_TIMEOUT;
		}
	}
	
	private static boolean shouldObserve()
	{
		try
		{
			String shouldObserveProp = System.getProperty(API_OBSERVE_PROPERTY);
			
			if (Strings.isNullOrEmpty(shouldObserveProp))
			{
				return DEFAULT_OBSERVE;
			}
			
			return Boolean.parseBoolean(shouldObserveProp);
		}
		catch (Exception e)
		{
			return DEFAULT_OBSERVE;
		}
	}
}
