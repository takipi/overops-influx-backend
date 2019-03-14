package com.takipi.integrations.grafana.cache.key.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.takipi.api.client.ApiClient;
import com.takipi.api.core.url.UrlClient.Response;

public abstract class CacheKey
{
	private static final Logger logger = LoggerFactory.getLogger(CacheKey.class);
	private static boolean PRINT_DURATIONS = true;
	
	protected final ApiClient apiClient;
	
	protected CacheKey(ApiClient apiClient)
	{
		this.apiClient = apiClient;
	}
	
	public ApiClient getApiClient()
	{
		return apiClient;
	}
	
	protected boolean printDuration()
	{
		return PRINT_DURATIONS;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (o == this)
		{
			return true;
		}
		
		if (!(o instanceof CacheKey))
		{
			return false;
		}
		
		CacheKey other = (CacheKey)o;
		
		return ((apiClient.getApiVersion() == other.apiClient.getApiVersion()) &&
				(Objects.equal(apiClient.getHostname(), other.apiClient.getHostname())) &&
				(Objects.equal(getServiceId(), other.getServiceId())));
	}
	
	@Override
	public int hashCode()
	{
		return Strings.nullToEmpty(apiClient.getHostname()).hashCode() ^ getServiceId().hashCode();
	}
	
	public Response<?> load()
	{
		long t1 = System.currentTimeMillis();
		
		try
		{
			Response<?> result = internalLoad();
			
			long t2 = System.currentTimeMillis();
			
			if (printDuration())
			{
				double sec = (double) (t2 - t1) / 1000;
				logger.info(sec + " sec: " + toString());
			}
			
			return result;
		}
		catch (Throwable e)
		{
			long t2 = System.currentTimeMillis();
			
			throw new IllegalStateException("Error executing after " + ((double) (t2 - t1) / 1000) + " sec: " + toString(), e);
		}
	}
	
	protected abstract String getServiceId();
	protected abstract Response<?> internalLoad();
}
