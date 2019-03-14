package com.takipi.integrations.grafana.cache.key.events;

import java.util.Collection;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.event.EventsVolumeRequest;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;

public class CacheGetEventsVolumeKey extends BaseCacheGetEventsKey
{
	private CacheGetEventsVolumeKey(ApiClient apiClient, EventsVolumeRequest request)
	{
		super(apiClient, request);
	}
	
	private EventsVolumeRequest eventsRequest()
	{
		return (EventsVolumeRequest) request;
	}
	
	@Override
	public boolean equals(Object o)
	{
		// This is here explicitly so this comment is visible.
		//
		// The reason we trust the parent compare is that EventsRequest and EventsVolumeRequest
		// both return the same result type EventsResult, and the volume request just lets the
		// caller choose the volume explicitly.
		//
		return super.equals(o);
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
	
	@Override
	protected VolumeType volumeType()
	{
		return eventsRequest().volumeType;
	}
	
	public static CacheKey create(ApiClient apiClient, EventsVolumeRequest request)
	{
		return new CacheGetEventsVolumeKey(apiClient, request);
	}
}
