package com.takipi.integrations.grafana.functions;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.takipi.api.client.ApiClient;

public class GrafanaThreadPool {
	
	private static final int CACHE_SIZE = 100;
	private static final int CACHE_RETENTION = 2;
	private static final int THREADS_PER_API_KEY = 10;
	
	private static final LoadingCache<ApiClient, Executor> executorCache = CacheBuilder.newBuilder().
			maximumSize(CACHE_SIZE).
			expireAfterWrite(CACHE_RETENTION, TimeUnit.MINUTES).
			expireAfterAccess(CACHE_RETENTION, TimeUnit.MINUTES).
			build(new CacheLoader<ApiClient, Executor>() {
				
				@Override
				public Executor load(ApiClient key) {
					Executor result = Executors.newFixedThreadPool(THREADS_PER_API_KEY);
					return result;
				}
			});
	
	public static Executor getExecutor(ApiClient apiClient) {
		try
		{
			return executorCache.get(apiClient);
		}
		catch (ExecutionException e)
		{
			throw new IllegalStateException(e);
		}
	}
}
