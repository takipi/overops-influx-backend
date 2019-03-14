package com.takipi.integrations.grafana.cache.key.events;

import java.util.Collection;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.event.EventsSlimVolumeRequest;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;
import com.takipi.integrations.grafana.cache.key.timeframe.CacheGetTimeframeKey;

public class CacheGetEventsSlimVolumeKey extends CacheGetTimeframeKey
{
	private CacheGetEventsSlimVolumeKey(ApiClient apiClient, EventsSlimVolumeRequest request)
	{
		super(apiClient, request);
	}
	
	private EventsSlimVolumeRequest eventsRequest()
	{
		return (EventsSlimVolumeRequest) request;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (!super.equals(o))
		{
			return false;
		}
		
		if (!(o instanceof CacheGetEventsSlimVolumeKey))
		{
			return false;
		}
		
		CacheGetEventsSlimVolumeKey other = (CacheGetEventsSlimVolumeKey) o;
		
		return (volumeType() == other.volumeType());
	}
	
	@Override
	protected String getServiceId()
	{
		return eventsRequest().serviceId;
	}
	
	@Override
	protected DateTime from()
	{
		return DateTime.parse(eventsRequest().from);
	}
	
	@Override
	protected DateTime to()
	{
		return DateTime.parse(eventsRequest().to);
	}
	
	@Override
	protected Collection<String> servers()
	{
		return eventsRequest().servers;
	}
	
	@Override
	protected Collection<String> applications()
	{
		return eventsRequest().apps;
	}
	
	@Override
	protected Collection<String> deployments()
	{
		return eventsRequest().deployments;
	}
	
	@Override
	protected String viewId()
	{
		return eventsRequest().viewId;
	}
	
	@Override
	protected boolean raw()
	{
		return eventsRequest().raw;
	}
	
	private VolumeType volumeType()
	{
		return eventsRequest().volumeType;
	}
	
	public static CacheKey create(ApiClient apiClient, EventsSlimVolumeRequest request)
	{
		return new CacheGetEventsSlimVolumeKey(apiClient, request);
	}
}
