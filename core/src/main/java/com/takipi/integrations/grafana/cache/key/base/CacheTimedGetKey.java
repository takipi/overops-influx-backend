package com.takipi.integrations.grafana.cache.key.base;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.core.request.intf.ApiGetRequest;
import com.takipi.integrations.grafana.cache.key.util.KeyDateUtil;

public abstract class CacheTimedGetKey extends CacheGetKey
{
	private final DateTime baselineTime;
	
	protected CacheTimedGetKey(ApiClient apiClient, ApiGetRequest<?> request)
	{
		super(apiClient, request);
		
		this.baselineTime = createBaselineTime();
	}
	
	private DateTime createBaselineTime()
	{
		return KeyDateUtil.roundDate(DateTime.now(), roundMinutesFactor());
	}
	
	// The nearest minute to round to.
	//
	protected abstract int roundMinutesFactor();
	
	@Override
	public boolean equals(Object o)
	{
		if (!super.equals(o))
		{
			return false;
		}
		
		if (!(o instanceof CacheTimedGetKey))
		{
			return false;
		}
		
		CacheTimedGetKey other = (CacheTimedGetKey) o;
		
		return baselineTime.equals(other.baselineTime);
	}
}
