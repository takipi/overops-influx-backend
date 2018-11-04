package com.takipi.integrations.grafana.utils;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.api.core.request.intf.ApiGetRequest;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.integrations.grafana.input.ViewInput;

public class ApiCache {

	private static final int CACHE_SIZE = 1000;
	private static final int CACHE_RETENTION = 2;

	public static class CacheKey {

		protected ApiClient apiClient;
		protected ApiGetRequest<?> request;

		public CacheKey(ApiClient apiClient, ApiGetRequest<?> request) {
			this.apiClient = apiClient;
			this.request = request;
		}

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof CacheKey)) {
				return false;
			}

			CacheKey other = (CacheKey) obj;

			if (!Objects.equal(apiClient.getHostname(), other.apiClient.getHostname())) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			return apiClient.getHostname().hashCode();
		}
	}

	public static class ServiceCacheKey extends CacheKey {

		protected String serviceId;

		public ServiceCacheKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId) {
			super(apiClient, request);
			this.serviceId = serviceId;
		}

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof ServiceCacheKey)) {
				return false;
			}

			ServiceCacheKey other = (ServiceCacheKey) obj;

			if (!super.equals(obj)) {
				return false;
			}

			if (!Objects.equal(serviceId, other.serviceId)) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			return apiClient.getHostname().hashCode();
		}
	}

	public static class ViewCacheKey extends ServiceCacheKey {

		protected ViewInput input;

		public ViewCacheKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input) {

			super(apiClient, request, serviceId);
			this.input = input;
		}
		
		private static boolean compare(Collection<String> a, Collection<String> b) {
			
			if (a.size() != b.size()) {
				return false;
			}
				
			for (String s : a) {
				if (!b.contains(s)) {
					return false;
				}
			}
			
			return true;
		}

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof ViewCacheKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			ViewCacheKey other = (ViewCacheKey) obj;

			if (!Objects.equal(input.timeFilter, other.input.timeFilter)) {
				return false;
			}

			if (!Objects.equal(input.view, other.input.view)) {
				return false;
			}

			if (!compare(input.getDeployments(serviceId), other.input.getDeployments(serviceId))) {
				return false;
			}
			
			if (!compare(input.getServers(serviceId), other.input.getServers(serviceId))) {
				return false;
			}
			
			if (!compare(input.getApplications(serviceId), other.input.getApplications(serviceId))) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			return serviceId.hashCode() ^ input.view.hashCode();
		}
		
		@Override
		public String toString() {
			return this.getClass().getSimpleName() + ": " + serviceId 
					+ " " + input.view + " D: " + input.deployments + " A: " + input.applications
					+ " S: " + input.servers;
		}
	}
	
	public static class EventKey extends ViewCacheKey {

		protected VolumeType volumeType;

		public EventKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input,
				VolumeType volumeType) {

			super(apiClient, request, serviceId, input);
			this.volumeType = volumeType;
		}
		
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof EventKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			EventKey other = (EventKey) obj;

			if (!Objects.equal(volumeType, other.volumeType)) {
				return false;
			}

			return true;
		}
		
		@Override
		public String toString() {
			return super.toString() + " " + volumeType;
		}
	}

	public static class GraphKey extends EventKey {

		protected int pointsWanted;

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof GraphKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			GraphKey other = (GraphKey) obj;

			if (pointsWanted != other.pointsWanted) {
				return false;
			}

			return true;
		}

		public GraphKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input,
				VolumeType volumeType, int pointsWanted) {

			super(apiClient, request, serviceId, input, volumeType);
			this.pointsWanted = pointsWanted;
		}
	}
	
	public static class TransactionsCacheKey extends ViewCacheKey {

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof TransactionsCacheKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			return true;
		}

		public TransactionsCacheKey(ApiClient apiClient, ApiGetRequest<?> request, 
			String serviceId, ViewInput input) {

			super(apiClient, request, serviceId, input);
		}
	}
	
	public static class TransactionsGraphCacheKey extends ViewCacheKey {

		protected int pointsWanted;

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof TransactionsCacheKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}
			
			TransactionsGraphCacheKey other = (TransactionsGraphCacheKey) obj;

			if (pointsWanted != other.pointsWanted) {
				return false;
			}

			return true;
		}

		public TransactionsGraphCacheKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input,
				int pointsWanted) {
			
			super(apiClient, request, serviceId, input);
			this.pointsWanted = pointsWanted;
		}
	}

	public static Response<?> getItem(CacheKey key) {
		try {
			return cache.get(key);
		} catch (ExecutionException e) {
			throw new IllegalStateException(e);
		}
	}

	private static final LoadingCache<CacheKey, Response<?>> cache = CacheBuilder.newBuilder().maximumSize(CACHE_SIZE)
			.expireAfterWrite(CACHE_RETENTION, TimeUnit.MINUTES).build(new CacheLoader<CacheKey, Response<?>>() {
				public Response<?> load(CacheKey key) {
					Response<?> result = key.apiClient.get(key.request);
					return result;
				}
			});

}
