package com.takipi.integrations.grafana.cache.key.clients;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.application.ApplicationsRequest;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;
import com.takipi.integrations.grafana.cache.key.base.CacheTimedGetKey;

public class CacheGetApplicationsKey extends CacheTimedGetKey
{
	private CacheGetApplicationsKey(ApiClient apiClient, ApplicationsRequest request)
	{
		super(apiClient, request);
	}
	
	private ApplicationsRequest applicationsRequest()
	{
		return (ApplicationsRequest) request;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (!super.equals(o))
		{
			return false;
		}
		
		if (!(o instanceof CacheGetApplicationsKey))
		{
			return false;
		}
		
		CacheGetApplicationsKey other = (CacheGetApplicationsKey) o;
		
		return (applicationsRequest().active == other.applicationsRequest().active);
	}
	
	@Override
	protected int roundMinutesFactor()
	{
		return 5;
	}
	
	@Override
	protected String getServiceId()
	{
		return applicationsRequest().serviceId;
	}
	
	public static CacheKey create(ApiClient apiClient, ApplicationsRequest request)
	{
		return new CacheGetApplicationsKey(apiClient, request);
	}
}
