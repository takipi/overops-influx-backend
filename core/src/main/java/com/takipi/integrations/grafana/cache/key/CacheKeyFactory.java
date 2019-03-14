package com.takipi.integrations.grafana.cache.key;

import com.takipi.api.client.ApiClient;
import com.takipi.api.core.request.intf.ApiGetRequest;
import com.takipi.api.core.result.intf.ApiResult;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;

public class CacheKeyFactory
{
	public static <T extends ApiResult> CacheKey getKey(ApiClient apiClient, ApiGetRequest<T> request)
	{
		return null;
	}
}
