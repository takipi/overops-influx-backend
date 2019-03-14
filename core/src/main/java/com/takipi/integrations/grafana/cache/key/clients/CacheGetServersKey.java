package com.takipi.integrations.grafana.cache.key.clients;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.server.ServersRequest;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;
import com.takipi.integrations.grafana.cache.key.base.CacheTimedGetKey;

public class CacheGetServersKey extends CacheTimedGetKey
{
	private CacheGetServersKey(ApiClient apiClient, ServersRequest request)
	{
		super(apiClient, request);
	}
	
	private ServersRequest serversRequest()
	{
		return (ServersRequest) request;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (!super.equals(o))
		{
			return false;
		}
		
		if (!(o instanceof CacheGetServersKey))
		{
			return false;
		}
		
		CacheGetServersKey other = (CacheGetServersKey) o;
		
		return (serversRequest().active == other.serversRequest().active);
	}
	
	@Override
	protected int roundMinutesFactor()
	{
		return 5;
	}
	
	@Override
	protected String getServiceId()
	{
		return serversRequest().serviceId;
	}
	
	public static CacheKey create(ApiClient apiClient, ServersRequest request)
	{
		return new CacheGetServersKey(apiClient, request);
	}
}
