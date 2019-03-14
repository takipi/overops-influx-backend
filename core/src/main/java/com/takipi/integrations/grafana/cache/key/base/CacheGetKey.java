package com.takipi.integrations.grafana.cache.key.base;

import com.takipi.api.client.ApiClient;
import com.takipi.api.core.request.intf.ApiGetRequest;
import com.takipi.api.core.url.UrlClient.Response;

public abstract class CacheGetKey extends CacheKey
{
	protected final ApiGetRequest<?> request;
	
	protected CacheGetKey(ApiClient apiClient, ApiGetRequest<?> request)
	{
		super(apiClient);
		
		this.request = request;
	}
	
	@Override
	protected Response<?> internalLoad()
	{
		return apiClient.get(request);
	}
}
