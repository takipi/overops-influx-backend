package com.takipi.integrations.grafana.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.observe.Observer;
import com.takipi.api.core.request.intf.ApiDeleteRequest;
import com.takipi.api.core.request.intf.ApiGetRequest;
import com.takipi.api.core.request.intf.ApiPostBytesRequest;
import com.takipi.api.core.request.intf.ApiPostRequest;
import com.takipi.api.core.request.intf.ApiPutRequest;
import com.takipi.api.core.result.intf.ApiResult;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.integrations.grafana.cache.key.CacheKeyFactory;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;
import com.takipi.integrations.grafana.functions.GrafanaThreadPool;

public class CachedApiClient implements ApiClient
{
	private static final int CACHE_SIZE = 1000;
	private static final int CACHE_REFRESH_RETENTION = 2;
	
	private static final LoadingCache<CacheKey, Response<?>> queryCache = CacheBuilder.newBuilder()
			.maximumSize(CACHE_SIZE)
			.expireAfterAccess(CACHE_REFRESH_RETENTION, TimeUnit.MINUTES)
			.refreshAfterWrite(CACHE_REFRESH_RETENTION, TimeUnit.MINUTES)
			.build(new CacheLoader<CacheKey, Response<?>>()
			{
				@Override
				public Response<?> load(CacheKey key)
				{
					Response<?> result = key.load();
					return result;
				}
				
				@Override
				public ListenableFuture<Response<?>> reload(final CacheKey key, Response<?> prev)
				{
					ListenableFutureTask<Response<?>> task = ListenableFutureTask.create(new Callable<Response<?>>()
					{
						@Override
						public Response<?> call() {
							return key.load();
						}
					});
					
					Executor executor = GrafanaThreadPool.getQueryExecutor(key.getApiClient());
					executor.execute(task);

					return task;

				}
			});
	
	private final ApiClient internalApiClient;
	
	private CachedApiClient(ApiClient internalApiClient)
	{
		this.internalApiClient = internalApiClient;
	}
	
	@Override
	public int getApiVersion()
	{
		return internalApiClient.getApiVersion();
	}
	
	@Override
	public String getHostname()
	{
		return internalApiClient.getHostname();
	}
	
	@Override
	public <T extends ApiResult> Response<T> get(ApiGetRequest<T> request)
	{
		CacheKey key = CacheKeyFactory.getKey(internalApiClient, request);
		
		if (key != null)
		{
			return getFromCache(key);
		}
		
		return internalApiClient.get(request);
	}
	
	@Override
	public <T extends ApiResult> Response<T> put(ApiPutRequest<T> request)
	{
		return internalApiClient.put(request);
	}
	
	@Override
	public <T extends ApiResult> Response<T> post(ApiPostRequest<T> request)
	{
		return internalApiClient.post(request);
	}
	
	@Override
	public <T extends ApiResult> Response<T> post(ApiPostBytesRequest<T> request)
	{
		return internalApiClient.post(request);
	}
	
	@Override
	public <T extends ApiResult> Response<T> delete(ApiDeleteRequest<T> request)
	{
		return internalApiClient.delete(request);
	}
	
	@Override
	public void addObserver(Observer observer)
	{
		internalApiClient.addObserver(observer);
	}
	
	private <T extends ApiResult> Response<T> getFromCache(CacheKey key)
	{
		try
		{
			Response<?> result = queryCache.get(key);
			
			if (result.isBadResponse())
			{
				queryCache.invalidate(key);
			} 
			
			return (Response<T>) result;
		}
		catch (ExecutionException e)
		{
			throw new IllegalStateException(e);
		}
	}
	
	public static ApiClient create(ApiClient internalApiClient)
	{
		return new CachedApiClient(internalApiClient);
	}
}
