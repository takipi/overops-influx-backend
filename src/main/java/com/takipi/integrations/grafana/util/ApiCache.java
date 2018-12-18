package com.takipi.integrations.grafana.util;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.event.EventsRequest;
import com.takipi.api.client.request.event.EventsSlimVolumeRequest;
import com.takipi.api.client.request.metrics.GraphRequest;
import com.takipi.api.client.request.transaction.TransactionsGraphRequest;
import com.takipi.api.client.request.transaction.TransactionsVolumeRequest;
import com.takipi.api.client.request.view.ViewsRequest;
import com.takipi.api.client.result.event.EventsSlimVolumeResult;
import com.takipi.api.client.result.metrics.GraphResult;
import com.takipi.api.client.result.transaction.TransactionsGraphResult;
import com.takipi.api.client.result.transaction.TransactionsVolumeResult;
import com.takipi.api.client.result.view.ViewsResult;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.api.core.request.intf.ApiGetRequest;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.integrations.grafana.functions.RegressionFunction;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.EventFilterInput;
import com.takipi.integrations.grafana.input.ViewInput;

public class ApiCache {

	private static final int CACHE_SIZE = 1000;
	private static final int CACHE_RETENTION = 2;

	public static boolean PRINT_DURATIONS = true;
	
	protected abstract static class CacheKey {

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

			if (!Objects.equal(apiClient, other.apiClient)) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			return apiClient.getHostname().hashCode();
		}
	}

	protected abstract static class ServiceCacheKey extends CacheKey {

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
			return super.hashCode() ^ serviceId.hashCode();
		}
	}

	protected static class ViewNameCacheKey extends ServiceCacheKey {

		protected String viewName;

		public ViewNameCacheKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, String viewName) {

			super(apiClient, request, serviceId);
			this.viewName = viewName;
		}

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof ViewNameCacheKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			ViewNameCacheKey other = (ViewNameCacheKey) obj;

			if (!Objects.equal(viewName, other.viewName)) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {

			if (viewName == null) {
				return super.hashCode();
			}

			return super.hashCode() ^ viewName.hashCode();
		}
		
		@Override
		public String toString() {
			return this.getClass().getSimpleName() + ": " + viewName + " " + viewName;
		}
	}

	protected abstract static class ViewCacheKey extends ServiceCacheKey {

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

			Collection<String> apps = input.getApplications(apiClient, serviceId);
			Collection<String> otherApps = other.input.getApplications(apiClient, serviceId);
			
			if (!compare(apps, otherApps)) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {

			if (input.view == null) {
				return super.hashCode();
			}

			return super.hashCode() ^ input.view.hashCode();
		}

		@Override
		public String toString() {
			return this.getClass().getSimpleName() + ": " + serviceId + " " + input.view + " D: " + input.deployments
					+ " A: " + input.applications + " S: " + input.servers;
		}
	}

	protected abstract static class VolumeKey extends ViewCacheKey {

		protected VolumeType volumeType;

		public VolumeKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input,
				VolumeType volumeType) {

			super(apiClient, request, serviceId, input);
			this.volumeType = volumeType;
		}

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof VolumeKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			VolumeKey other = (VolumeKey) obj;
			
			if (volumeType != null) {
				
				switch (volumeType) {
					case hits: 
						if ((other.volumeType == null) || (other.volumeType.equals(VolumeType.invocations))) {
							return false;
						}
						break;
					
					case invocations: 
						if ((other.volumeType == null) || (other.volumeType.equals(VolumeType.hits))) {
							return false;
						}
						break;
					case all: 
						if ((other.volumeType == null) || (!other.volumeType.equals(VolumeType.all))) {
							return false;
						}
						break;
				}
			}
			
			if (!Objects.equal(volumeType, other.volumeType)) {
				return false;
			}

			return true;
		}

		@Override
		public String toString() {
			return super.toString() + " vt: " + volumeType;
		}
	}
	
	protected static class SlimEventKey extends VolumeKey {
		
		public SlimEventKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input,
				VolumeType volumeType) {

			super(apiClient, request, serviceId, input, volumeType);
		}
		
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof SlimEventKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			return true;
		}
	}


	protected static class EventKey extends VolumeKey {
		
		public EventKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input,
				VolumeType volumeType) {

			super(apiClient, request, serviceId, input, volumeType);
		}
		
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof EventKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			return true;
		}
	}

	protected static class GraphKey extends VolumeKey {

		protected int pointsWanted;
		protected int activeWindow;
		protected int baselineWindow;
		protected int windowSlice;

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof GraphKey)) {
				return false;
			}

			if (!super.equals( obj)) {
				return false;
			}

			GraphKey other = (GraphKey) obj;

			if (pointsWanted != other.pointsWanted) {
				return false;
			}
			
			if (activeWindow != other.activeWindow) {
				return false;
			}
			
			if (baselineWindow != other.baselineWindow) {
				return false;
			}
			

			if (windowSlice != other.windowSlice) {
				return false;
			}

			return true;
		}

		public GraphKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input,
				VolumeType volumeType, int pointsWanted, int baselineWindow, int activeWindow, int windowSlice) {

			super(apiClient, request, serviceId, input, volumeType);
			this.pointsWanted = pointsWanted;
			this.activeWindow = activeWindow;
			this.baselineWindow = baselineWindow;
			this.windowSlice = windowSlice;
			
			if (pointsWanted == 0) {
				System.out.println();
			}
		}
		
		@Override
		public String toString()
		{
			String result = super.toString() + " pw: " + pointsWanted + " aw: " 
				+ activeWindow + " bw: " + baselineWindow + " slc: " + windowSlice;
			
			return result;
		}
	}

	protected static class TransactionsCacheKey extends ViewCacheKey {

		protected int baselineTimespan;
		protected int activeTimespan;
		
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof TransactionsCacheKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}
			
			TransactionsCacheKey other = (TransactionsCacheKey)obj;
			
			if (activeTimespan != other.activeTimespan) {
				return false;
			}
			
			if (baselineTimespan != other.baselineTimespan) {
				return false;
			}

			return true;
		}

		public TransactionsCacheKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input) {

			this(apiClient, request, serviceId, input, 0, 0);
		}
		
		public TransactionsCacheKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input, int baselineTimespan, int activeTimespan) {
			super(apiClient, request, serviceId, input);
			
			this.baselineTimespan = baselineTimespan;
			this.activeTimespan = activeTimespan;
		}
	}

	protected static class TransactionsGraphCacheKey extends ViewCacheKey {

		protected int pointsWanted;
		protected int baselineTimespan;
		protected int activeTimespan;

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof TransactionsGraphCacheKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			TransactionsGraphCacheKey other = (TransactionsGraphCacheKey) obj;

			if (pointsWanted != other.pointsWanted) {
				return false;
			}
			
			if (baselineTimespan != other.baselineTimespan) {
				return false;
			}
			
			if (activeTimespan != other.activeTimespan) {
				return false;
			}

			return true;
		}
		
		@Override
		public String toString()
		{
			return super.toString() + " aw = " + activeTimespan + " bw = " + baselineTimespan;
		}

		public TransactionsGraphCacheKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId,
				ViewInput input, int pointsWanted) {
			this(apiClient, request, serviceId, input, pointsWanted, 0, 0);

		}

		public TransactionsGraphCacheKey(ApiClient apiClient, ApiGetRequest<?> request, String serviceId,
				ViewInput input, int pointsWanted, int baselineTimespan, int activeTimespan) {

			super(apiClient, request, serviceId, input);
			this.pointsWanted = pointsWanted;
			this.activeTimespan = activeTimespan;
			this.baselineTimespan = baselineTimespan;
		}
	}
	
	private static class RegresionWindowKey {
		protected RegressionInput input;
		protected ApiClient apiClient;

		protected RegresionWindowKey(ApiClient apiClient, RegressionInput input) {
			this.input = input;
			this.apiClient = apiClient;
		}

		@Override
		public boolean equals(Object obj) {

			RegresionWindowKey other = (RegresionWindowKey) obj;

			if (!apiClient.getHostname().equals(other.apiClient.getHostname())) {
				return false;
			}

			if (!input.serviceId.equals(other.input.serviceId)) {
				return false;
			}

			if (!input.viewId.equals(other.input.viewId)) {
				return false;
			}

			if (input.activeTimespan != other.input.activeTimespan) {
				return false;
			}

			if (input.baselineTimespan != other.input.baselineTimespan) {
				return false;
			}

			if ((input.deployments == null) != (other.input.deployments == null)) {
				return false;
			}

			if (input.deployments != null) {
				
				if (input.deployments.size() != other.input.deployments.size()) {
					return false;
				}
	
				for (String dep : input.deployments) {
					if (!other.input.deployments.contains(dep)) {
						return false;
					}
				}
			}

			return true;
		}

		@Override
		public int hashCode() {

			StringBuilder result = new StringBuilder();

			result.append(input.serviceId).append(input.viewId);

			if (input.deployments != null) {

				for (String dep : input.deployments) {
					result.append(dep);
				}

			}

			return result.toString().hashCode();
		}
	}
	
	protected static class RegressionKey extends EventKey {

		protected RegressionFunction function;

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof RegressionKey)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			RegressionKey other = (RegressionKey) obj;
			
			EventFilterInput eventInput = (EventFilterInput)input;
			EventFilterInput otherInput = (EventFilterInput)(other.input);
			
			if (!Objects.equal(eventInput.types, otherInput.types)) {
				return false;
			}
						
			if (!Objects.equal(eventInput.hasTransactions(), otherInput.hasTransactions())) {
				return false;
			}
			
			if ((eventInput.hasTransactions()) && (!Objects.equal(eventInput.transactions, 
				otherInput.transactions))) {
				return false;
			}
			
			if (!Objects.equal(eventInput.searchText, otherInput.searchText)) {
				return false;
			}

			return true;
		}

		public RegressionKey(ApiClient apiClient, String serviceId, ViewInput input,
				RegressionFunction function) {

			super(apiClient, null, serviceId, input, null);
			
			this.function = function;
		}
	}

	private static Response<?> getItem(CacheKey key) {
		try {
			Response<?> result = queryCache.get(key);
			
			if (result.isBadResponse()) {
				queryCache.invalidate(key);
			} 
			
			return result;
		} catch (ExecutionException e) {
			throw new IllegalStateException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static Response<ViewsResult> getView(ApiClient apiClient, String serviceId, String viewName,
			ViewsRequest viewsRequest) {

		ViewNameCacheKey cacheKey = new ViewNameCacheKey(apiClient, viewsRequest, serviceId, viewName);
		Response<ViewsResult> response = (Response<ViewsResult>)getItem(cacheKey);

		return response;
	}
	
	public static Response<TransactionsVolumeResult> getTransactionsVolume(ApiClient apiClient, String serviceId,
			ViewInput input, TransactionsVolumeRequest request) {
		return getTransactionsVolume(apiClient, serviceId, input, 0, 0, request);
	}

	@SuppressWarnings("unchecked")
	public static Response<TransactionsVolumeResult> getTransactionsVolume(ApiClient apiClient, String serviceId,
			ViewInput input, int activeTimespan, int baselineTimespan, TransactionsVolumeRequest request) {

		TransactionsCacheKey cacheKey = new TransactionsCacheKey(apiClient, request, serviceId, input, activeTimespan, baselineTimespan);
		Response<TransactionsVolumeResult> response = (Response<TransactionsVolumeResult>)getItem(cacheKey);

		return response;
	}

	@SuppressWarnings("unchecked")
	public static Response<GraphResult> getEventGraph(ApiClient apiClient, String serviceId,
			ViewInput input, VolumeType volumeType, GraphRequest request, int pointsWanted,
			int baselineWindow, int activeWindow, int windowSlice) {

		GraphKey cacheKey = new GraphKey(apiClient, request, serviceId, input, volumeType, 
			pointsWanted, baselineWindow, activeWindow, windowSlice);
		Response<GraphResult> response = (Response<GraphResult>) getItem(cacheKey);

		return response;
	}
	
	public static void putEventGraph(ApiClient apiClient, String serviceId,
			ViewInput input, VolumeType volumeType, GraphRequest request, int pointsWanted,
			int baselineWindow, int activeWindow, int windowSlice, Response<GraphResult> graphResult) {

		GraphKey cacheKey = new GraphKey(apiClient, request, serviceId, input, volumeType, 
			pointsWanted, baselineWindow, activeWindow, windowSlice);
		
		queryCache.put(cacheKey, graphResult);
	}

	@SuppressWarnings("unchecked")
	public static Response<EventsSlimVolumeResult> getEventVolume(ApiClient apiClient, String serviceId, 
			ViewInput input, VolumeType volumeType, EventsSlimVolumeRequest request) {

		SlimEventKey cacheKey = new SlimEventKey(apiClient, request, serviceId, input, volumeType);
		Response<EventsSlimVolumeResult> response = (Response<EventsSlimVolumeResult>)getItem(cacheKey);
		return response;
	}

	private static Response<?> getEventList(ApiClient apiClient, String serviceId, 
			ViewInput input, EventsRequest request, VolumeType volumeType, boolean load) {
		
		EventKey cacheKey = new EventKey(apiClient, request, serviceId, input, volumeType);		
		Response<?> response;
		
		if (load) {
			response = getItem(cacheKey);
		} else {
			response = queryCache.getIfPresent(cacheKey);
		}
	
		return response;
	}
	
	public static Response<?> getEventList(ApiClient apiClient, String serviceId, 
			ViewInput input, EventsRequest request) {
		
		Response<?> response;
		
		for (VolumeType volumeType : VolumeType.values()) {
			
			response = getEventList(apiClient, serviceId, 
					input, request,volumeType, false);
			
			if (response != null) {
				return response;
			}
		}
		
		response = getEventList(apiClient, serviceId, 
				input, request,null, true);
		
		return response;
	}
	
	public static Response<TransactionsGraphResult> getTransactionsGraph(ApiClient apiClient, String serviceId,
			BaseGraphInput input, int pointsWanted,
			TransactionsGraphRequest request) {
		return getTransactionsGraph(apiClient, serviceId, input, pointsWanted, 0, 0, request);
	}

	@SuppressWarnings("unchecked")
	public static Response<TransactionsGraphResult> getTransactionsGraph(ApiClient apiClient, String serviceId,
			EventFilterInput input, int pointsWanted, int baselineTimespan, int activeTimespan, 
			TransactionsGraphRequest request) {

		TransactionsGraphCacheKey cacheKey = new TransactionsGraphCacheKey(apiClient, request, serviceId, input,
				pointsWanted, baselineTimespan, activeTimespan);
		Response<TransactionsGraphResult> response = (Response<TransactionsGraphResult>) ApiCache.getItem(cacheKey);
		
		return response;
	}

	public static RegressionWindow getRegressionWindow(ApiClient apiClient, RegressionInput input) {

		try {
			return regressionWindowCache.get(new RegresionWindowKey(apiClient, input));

		} catch (ExecutionException e) {
			throw new IllegalStateException(e);
		}
	}
	
	public static RegressionOutput getRegressionOutput(ApiClient apiClient, String serviceId, 
		EventFilterInput input, RegressionFunction function, boolean load) {
			
		RegressionKey key = new RegressionKey(apiClient, serviceId, input, function);
		
		if (load) {
			try
			{
				RegressionOutput result = rgressionReportRache.get(key);
				
				if (result.empty) {
					rgressionReportRache.invalidate(key);
				}
				
				return result;
			}
			catch (ExecutionException e)
			{
				throw new IllegalStateException(e);
			}
		} else {
			return rgressionReportRache.getIfPresent(key);
		}
	}
	
	private static final LoadingCache<RegressionKey, RegressionOutput> rgressionReportRache = CacheBuilder
			.newBuilder().maximumSize(CACHE_SIZE).expireAfterWrite(CACHE_RETENTION, TimeUnit.MINUTES)
			.build(new CacheLoader<RegressionKey, RegressionOutput>() {
				
				@Override
				public RegressionOutput load(RegressionKey key) {
					return key.function.executeRegression(key.serviceId, (EventFilterInput)key.input);
				}
			});

	private static final LoadingCache<RegresionWindowKey, RegressionWindow> regressionWindowCache = CacheBuilder
			.newBuilder().maximumSize(CACHE_SIZE).expireAfterWrite(CACHE_RETENTION, TimeUnit.MINUTES)
			.build(new CacheLoader<RegresionWindowKey, RegressionWindow>() {
				
				@Override
				public RegressionWindow load(RegresionWindowKey key) {
					
					RegressionWindow result = RegressionUtil.getActiveWindow(key.apiClient, key.input, 4,
							System.out);
					return result;
				}
			});

	private static final LoadingCache<CacheKey, Response<?>> queryCache = CacheBuilder.newBuilder()
			.maximumSize(CACHE_SIZE).expireAfterWrite(CACHE_RETENTION, TimeUnit.MINUTES)
			.build(new CacheLoader<CacheKey, Response<?>>() {
				
				@Override
				public Response<?> load(CacheKey key) {
					long t1 = System.currentTimeMillis();
					
					try {
						Response<?> result = key.apiClient.get(key.request);
						
						long t2 = System.currentTimeMillis();
						
						if (PRINT_DURATIONS) {
							double sec = (double)(t2-t1) / 1000;
							System.out.println(sec + " sec: " + key);
						}
						
						return result;
 					} catch (Throwable e) {
 						long t2 = System.currentTimeMillis();
 						
 						throw new IllegalStateException("Error executing after " + ((double)(t2-t1) / 1000) + " sec: " + key, e);
 					}
				}
			});
}
