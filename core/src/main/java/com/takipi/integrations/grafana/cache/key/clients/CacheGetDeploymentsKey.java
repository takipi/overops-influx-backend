package com.takipi.integrations.grafana.cache.key.clients;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.deployment.DeploymentsRequest;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;
import com.takipi.integrations.grafana.cache.key.base.CacheTimedGetKey;

public class CacheGetDeploymentsKey extends CacheTimedGetKey
{
	private CacheGetDeploymentsKey(ApiClient apiClient, DeploymentsRequest request)
	{
		super(apiClient, request);
	}
	
	private DeploymentsRequest deploymentsRequest()
	{
		return (DeploymentsRequest) request;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (!super.equals(o))
		{
			return false;
		}
		
		if (!(o instanceof CacheGetDeploymentsKey))
		{
			return false;
		}
		
		CacheGetDeploymentsKey other = (CacheGetDeploymentsKey) o;
		
		return (deploymentsRequest().active == other.deploymentsRequest().active);
	}
	
	@Override
	protected int roundMinutesFactor()
	{
		return 5;
	}
	
	@Override
	protected String getServiceId()
	{
		return deploymentsRequest().serviceId;
	}
	
	public static CacheKey create(ApiClient apiClient, DeploymentsRequest request)
	{
		return new CacheGetDeploymentsKey(apiClient, request);
	}
}
