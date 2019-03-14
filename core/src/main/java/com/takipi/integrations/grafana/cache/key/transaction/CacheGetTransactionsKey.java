package com.takipi.integrations.grafana.cache.key.transaction;

import java.util.Collection;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.transaction.TransactionsVolumeRequest;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;
import com.takipi.integrations.grafana.cache.key.timeframe.CacheGetTimeframeKey;

public class CacheGetTransactionsKey extends CacheGetTimeframeKey
{
	private CacheGetTransactionsKey(ApiClient apiClient, TransactionsVolumeRequest request)
	{
		super(apiClient, request);
	}
	
	private TransactionsVolumeRequest transactionsRequest()
	{
		return (TransactionsVolumeRequest) request;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (!super.equals(o))
		{
			return false;
		}
		
		if (!(o instanceof CacheGetTransactionsKey))
		{
			return false;
		}
		
		return true;
	}
	
	@Override
	protected String getServiceId()
	{
		return transactionsRequest().serviceId;
	}
	
	@Override
	protected DateTime from()
	{
		return DateTime.parse(transactionsRequest().from);
	}
	
	@Override
	protected DateTime to()
	{
		return DateTime.parse(transactionsRequest().to);
	}
	
	@Override
	protected Collection<String> servers()
	{
		return transactionsRequest().servers;
	}
	
	@Override
	protected Collection<String> applications()
	{
		return transactionsRequest().apps;
	}
	
	@Override
	protected Collection<String> deployments()
	{
		return transactionsRequest().deployments;
	}
	
	@Override
	protected String viewId()
	{
		return transactionsRequest().viewId;
	}
	
	@Override
	protected boolean raw()
	{
		return transactionsRequest().raw;
	}
	
	public static CacheKey create(ApiClient apiClient, TransactionsVolumeRequest request)
	{
		return new CacheGetTransactionsKey(apiClient, request);
	}
}
