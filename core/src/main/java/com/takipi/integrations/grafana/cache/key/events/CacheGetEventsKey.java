package com.takipi.integrations.grafana.cache.key.events;

import java.util.Collection;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.event.EventsRequest;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;

public class CacheGetEventsKey extends BaseCacheGetEventsKey
{
	private CacheGetEventsKey(ApiClient apiClient, EventsRequest request)
	{
		super(apiClient, request);
	}
	
	private EventsRequest eventsRequest()
	{
		return (EventsRequest) request;
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
		// This is hard coded in EventsRequest.
		//
		return VolumeType.hits;
	}
	
	public static CacheKey create(ApiClient apiClient, EventsRequest request)
	{
		return new CacheGetEventsKey(apiClient, request);
	}
}
