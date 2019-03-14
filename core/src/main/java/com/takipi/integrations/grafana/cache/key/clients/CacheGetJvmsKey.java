package com.takipi.integrations.grafana.cache.key.clients;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.process.JvmsRequest;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;
import com.takipi.integrations.grafana.cache.key.base.CacheTimedGetKey;

public class CacheGetJvmsKey extends CacheTimedGetKey
{
	private CacheGetJvmsKey(ApiClient apiClient, JvmsRequest request)
	{
		super(apiClient, request);
	}
	
	private JvmsRequest jvmsRequest()
	{
		return (JvmsRequest) request;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (!super.equals(o))
		{
			return false;
		}
		
		if (!(o instanceof CacheGetJvmsKey))
		{
			return false;
		}
		
		CacheGetJvmsKey other = (CacheGetJvmsKey) o;
		
		return (jvmsRequest().connected == other.jvmsRequest().connected);
	}
	
	@Override
	protected int roundMinutesFactor()
	{
		return 5;
	}
	
	@Override
	protected String getServiceId()
	{
		return jvmsRequest().serviceId;
	}
	
	public static CacheKey create(ApiClient apiClient, JvmsRequest request)
	{
		return new CacheGetJvmsKey(apiClient, request);
	}
}
