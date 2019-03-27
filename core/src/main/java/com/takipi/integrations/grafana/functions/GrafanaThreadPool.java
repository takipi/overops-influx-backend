package com.takipi.integrations.grafana.functions;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;

public class GrafanaThreadPool {
	private static final int CACHE_SIZE = 100;
	private static final int CACHE_RETENTION_MIN = 2;
	private static final int THREADS_PER_API_KEY = 50;
	
	private static final LoadingCache<ApiClient, Pair<Executor, Executor>> executorCache = CacheBuilder
			.newBuilder()
			.maximumSize(CACHE_SIZE)
			.expireAfterWrite(CACHE_RETENTION_MIN, TimeUnit.MINUTES)
			.expireAfterAccess(CACHE_RETENTION_MIN, TimeUnit.MINUTES)
			.build(new CacheLoader<ApiClient, Pair<Executor, Executor>>() {
				
				@Override
				public Pair<Executor, Executor> load(ApiClient key) {
					
					//the thread pools must be separated to prevent deadlocking between 
					//a func query slice thats waiting on a thread, but the thread pool has been depleted
					//by other functions waiting on this query to come back from the guava cache 
					Executor functionExecutor = Executors.newFixedThreadPool(THREADS_PER_API_KEY / 2);
					Executor queryExecutor = Executors.newFixedThreadPool(THREADS_PER_API_KEY / 2);

					return Pair.of(functionExecutor, queryExecutor);
				}
			});
	
	public static Executor getQueryExecutor(ApiClient apiClient) {
		try
		{
			return executorCache.get(apiClient).getSecond();
		}
		catch (ExecutionException e)
		{
			throw new IllegalStateException(e);
		}
	}
	
	public static Executor getFunctionExecutor(ApiClient apiClient) {
		try
		{
			return executorCache.get(apiClient).getFirst();
		}
		catch (ExecutionException e)
		{
			throw new IllegalStateException(e);
		}
	}
}
