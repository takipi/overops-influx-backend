package com.takipi.integrations.grafana.cache.key.events;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.api.core.request.intf.ApiGetRequest;
import com.takipi.integrations.grafana.cache.key.timeframe.CacheGetTimeframeKey;

public abstract class BaseCacheGetEventsKey extends CacheGetTimeframeKey
{
	protected BaseCacheGetEventsKey(ApiClient apiClient, ApiGetRequest<?> request)
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
		
		if (!(o instanceof BaseCacheGetEventsKey))
		{
			return false;
		}
		
		BaseCacheGetEventsKey other = (BaseCacheGetEventsKey) o;
		
		return (volumeType() == other.volumeType());
	}
	
	protected abstract VolumeType volumeType();
}
