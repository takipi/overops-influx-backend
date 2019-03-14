package com.takipi.integrations.grafana.cache.key.timeframe;

import java.util.Collection;

import org.joda.time.DateTime;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.api.core.request.intf.ApiGetRequest;
import com.takipi.integrations.grafana.cache.key.base.CacheGetKey;
import com.takipi.integrations.grafana.cache.key.util.KeyDateUtil;

public abstract class CacheGetTimeframeKey extends CacheGetKey
{
	protected CacheGetTimeframeKey(ApiClient apiClient, ApiGetRequest<?> request)
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
		
		if (!(o instanceof CacheGetTimeframeKey))
		{
			return false;
		}
		
		CacheGetTimeframeKey other = (CacheGetTimeframeKey) o;
		
		return ((adjustedFrom().equals(other.adjustedFrom())) &&
				(adjustedTo().equals(other.adjustedTo())) &&
				(compareCollections(servers(), other.servers())) &&
				(compareCollections(applications(), other.applications())) &&
				(compareCollections(deployments(), other.deployments())) &&
				(Objects.equal(viewId(), other.viewId())) &&
				(raw() == other.raw()));
	}
	
	private DateTime adjustedFrom()
	{
		return KeyDateUtil.roundDate(from(), 1);
	}
	
	private DateTime adjustedTo()
	{
		return KeyDateUtil.roundDate(to(), 1);
	}
	
	protected abstract DateTime from();
	protected abstract DateTime to();
	protected abstract Collection<String> servers();
	protected abstract Collection<String> applications();
	protected abstract Collection<String> deployments();
	protected abstract String viewId();
	protected abstract boolean raw();
	
	private static boolean compareCollections(Collection<String> a, Collection<String> b)
	{
		if (a.size() != b.size())
		{
			return false;
		}
		
		for (String s : a)
		{
			if (!b.contains(s))
			{
				return false;
			}
		}
		
		return true;
	}
}
