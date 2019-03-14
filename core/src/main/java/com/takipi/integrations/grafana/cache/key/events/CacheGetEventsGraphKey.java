package com.takipi.integrations.grafana.cache.key.events;

import java.util.Collection;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.metrics.GraphRequest;
import com.takipi.api.client.util.validation.ValidationUtil.GraphResolution;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;
import com.takipi.integrations.grafana.cache.key.timeframe.CacheGetTimeframeGraphKey;

public class CacheGetEventsGraphKey extends CacheGetTimeframeGraphKey
{
	private CacheGetEventsGraphKey(ApiClient apiClient, GraphRequest request)
	{
		super(apiClient, request);
	}
	
	private GraphRequest graphRequest()
	{
		return (GraphRequest) request;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (!super.equals(o))
		{
			return false;
		}
		
		if (!(o instanceof CacheGetEventsGraphKey))
		{
			return false;
		}
		
		CacheGetEventsGraphKey other = (CacheGetEventsGraphKey) o;
		
		return ((graphRequest().graphType == other.graphRequest().graphType) &&
				(graphRequest().volumeType == other.graphRequest().volumeType));
	}
	
	@Override
	protected String getServiceId()
	{
		return graphRequest().serviceId;
	}
	
	@Override
	protected DateTime from()
	{
		return DateTime.parse(graphRequest().from);
	}
	
	@Override
	protected DateTime to()
	{
		return DateTime.parse(graphRequest().to);
	}
	
	@Override
	protected Collection<String> servers()
	{
		return graphRequest().servers;
	}
	
	@Override
	protected Collection<String> applications()
	{
		return graphRequest().apps;
	}
	
	@Override
	protected Collection<String> deployments()
	{
		return graphRequest().deployments;
	}
	
	@Override
	protected String viewId()
	{
		return graphRequest().viewId;
	}
	
	@Override
	protected boolean raw()
	{
		return graphRequest().raw;
	}
	
	@Override
	protected int pointsCount()
	{
		return graphRequest().wantedPointCount;
	}
	
	@Override
	protected GraphResolution graphResolution()
	{
		return graphRequest().resolution;
	}
	
	public static CacheKey create(ApiClient apiClient, GraphRequest request)
	{
		return new CacheGetEventsGraphKey(apiClient, request);
	}
}
