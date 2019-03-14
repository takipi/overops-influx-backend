package com.takipi.integrations.grafana.cache.key.timeframe;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.validation.ValidationUtil.GraphResolution;
import com.takipi.api.core.request.intf.ApiGetRequest;

public abstract class CacheGetTimeframeGraphKey extends CacheGetTimeframeKey
{
	protected CacheGetTimeframeGraphKey(ApiClient apiClient, ApiGetRequest<?> request)
	{
		super(apiClient, request);
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (!super.equals(o))
		{
			return false;
		}
		
		if (!(o instanceof CacheGetTimeframeGraphKey))
		{
			return false;
		}
		
		CacheGetTimeframeGraphKey other = (CacheGetTimeframeGraphKey) o;
		
		GraphResolution resolution = graphResolution();
		
		if (resolution != null)
		{
			return (resolution == other.graphResolution());
		}
		
		if (other.graphResolution() != null)
		{
			return false;
		}
		
		return (pointsCount() == other.pointsCount());
	}
	
	protected abstract int pointsCount();
	protected abstract GraphResolution graphResolution();
}
