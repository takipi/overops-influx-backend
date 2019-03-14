package com.takipi.integrations.grafana.cache.key.views;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.view.ViewsRequest;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;
import com.takipi.integrations.grafana.cache.key.base.CacheTimedGetKey;

public class CacheGetViewKey extends CacheTimedGetKey
{
	private CacheGetViewKey(ApiClient apiClient, ViewsRequest request)
	{
		super(apiClient, request);
	}
	
	private ViewsRequest viewsRequest()
	{
		return (ViewsRequest) request;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (!super.equals(o))
		{
			return false;
		}
		
		if (!(o instanceof CacheGetViewKey))
		{
			return false;
		}
		
		CacheGetViewKey other = (CacheGetViewKey) o;
		
		return Objects.equal(viewsRequest().viewName, other.viewsRequest().viewName);
	}
	
	@Override
	protected int roundMinutesFactor()
	{
		return 30;
	}
	
	@Override
	protected String getServiceId()
	{
		return viewsRequest().serviceId;
	}
	
	public static CacheKey create(ApiClient apiClient, ViewsRequest request)
	{
		return new CacheGetViewKey(apiClient, request);
	}
}
