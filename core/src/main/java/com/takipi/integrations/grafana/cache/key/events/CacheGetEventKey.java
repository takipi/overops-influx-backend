package com.takipi.integrations.grafana.cache.key.events;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.event.EventRequest;
import com.takipi.integrations.grafana.cache.key.base.CacheGetKey;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;

public class CacheGetEventKey extends CacheGetKey
{
	private CacheGetEventKey(ApiClient apiClient, EventRequest request)
	{
		super(apiClient, request);
	}
	
	private EventRequest eventRequest()
	{
		return (EventRequest) request;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (!super.equals(o))
		{
			return false;
		}
		
		if (!(o instanceof CacheGetEventKey))
		{
			return false;
		}
		
		CacheGetEventKey other = (CacheGetEventKey) o;
		
		return Objects.equal(eventRequest().eventId, other.eventRequest().eventId);
	}
	
	@Override
	protected String getServiceId()
	{
		return eventRequest().serviceId;
	}
	
	public static CacheKey create(ApiClient apiClient, EventRequest request)
	{
		return new CacheGetEventKey(apiClient, request);
	}
}
