package com.takipi.integrations.grafana.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.application.SummarizedApplication;
import com.takipi.api.client.request.application.ApplicationsRequest;
import com.takipi.api.client.request.deployment.DeploymentsRequest;
import com.takipi.api.client.request.event.EventRequest;
import com.takipi.api.client.request.event.EventsRequest;
import com.takipi.api.client.request.event.EventsSlimVolumeRequest;
import com.takipi.api.client.request.metrics.GraphRequest;
import com.takipi.api.client.request.transaction.TransactionsGraphRequest;
import com.takipi.api.client.request.transaction.TransactionsVolumeRequest;
import com.takipi.api.client.request.view.ViewsRequest;
import com.takipi.api.client.result.application.ApplicationsResult;
import com.takipi.api.client.result.deployment.DeploymentsResult;
import com.takipi.api.client.result.event.EventResult;
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
import com.takipi.integrations.grafana.functions.GrafanaThreadPool;
import com.takipi.integrations.grafana.functions.RegressionFunction;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.input.BaseEventVolumeInput;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.EventFilterInput;
import com.takipi.integrations.grafana.input.ViewInput;

public class ApiCache {
	private static final Logger logger = LoggerFactory.getLogger(ApiCache.class);
	
	private static final int CACHE_SIZE = 1000;
	private static final int CACHE_REFRESH_RETENTION = 2;
	private static final int CACHE_RELOAD_WINDOW = 3;
	
	public static boolean PRINT_DURATIONS = true;
	
	protected abstract static class BaseCacheLoader {

		protected ApiClient apiClient;
		protected ApiGetRequest<?> request;
		
		public BaseCacheLoader(ApiClient apiClient, ApiGetRequest<?> request) {
			this.apiClient = apiClient;
			this.request = request;
		}

		protected boolean printDuration() {
			return true;
		}
		
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof BaseCacheLoader)) {
				return false;
			}

			BaseCacheLoader other = (BaseCacheLoader) obj;

			if (!Objects.equal(apiClient, other.apiClient)) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			return apiClient.getHostname().hashCode();
		}
		
		public Response<?> load() {
			long t1 = System.currentTimeMillis();
				
			try {
				Response<?> result = apiClient.get(request);
				
				long t2 = System.currentTimeMillis();
				
				if ((PRINT_DURATIONS)  && (printDuration())) {
					double sec = (double)(t2-t1) / 1000;
					logger.info(sec + " sec: " + toString());
				}
				
				return result;
			} catch (Throwable e) {
				long t2 = System.currentTimeMillis();
				
				throw new IllegalStateException("Error executing after " + ((double)(t2-t1) / 1000) + " sec: " + toString(), e);
			}
		}
	}

	protected abstract static class ServiceCacheLoader extends BaseCacheLoader {

		protected String serviceId;

		public ServiceCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId) {
			super(apiClient, request);
			this.serviceId = serviceId;
		}

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof ServiceCacheLoader)) {
				return false;
			}

			ServiceCacheLoader other = (ServiceCacheLoader) obj;

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
	
	protected static class EventCacheLoader extends ServiceCacheLoader {

		protected String Id;

		public EventCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, String Id) {

			super(apiClient, request, serviceId);
			this.Id = Id;
		}
		
		@Override
		protected boolean printDuration()
		{
			return false;
		}
		
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof EventCacheLoader)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			EventCacheLoader other = (EventCacheLoader) obj;

			if (!Objects.equal(Id, other.Id)) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {

			if (Id == null) {
				return super.hashCode();
			}

			return super.hashCode() ^ Id.hashCode();
		}
		
		@Override
		public String toString() {
			return this.getClass().getSimpleName() + ": " + Id;
		}
	}

	protected static class DeploymentsCacheLoader extends ServiceCacheLoader {
		
		protected boolean active;
		
		public DeploymentsCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, 
			String serviceId, boolean active) {

			super(apiClient, request, serviceId);
			
			this.active = active;
		}
		
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof DeploymentsCacheLoader)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			DeploymentsCacheLoader other = (DeploymentsCacheLoader) obj;

			if (active != other.active) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			return super.hashCode();
		}
		
		@Override
		public String toString() {
			return this.getClass().getSimpleName() + " active: " + active;
		}
	}
	
	protected static class ApplicationsCacheLoader extends ServiceCacheLoader {
		
		protected boolean active;
		
		public ApplicationsCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, 
			String serviceId, boolean active) {

			super(apiClient, request, serviceId);
			
			this.active = active;
		}
		
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof ApplicationsCacheLoader)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			ApplicationsCacheLoader other = (ApplicationsCacheLoader) obj;

			if (active != other.active) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			return super.hashCode();
		}
		
		@Override
		public String toString() {
			return this.getClass().getSimpleName() + " active: " + active;
		}
	}
	
	protected static class ViewCacheLoader extends ServiceCacheLoader {

		protected String viewName;

		public ViewCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, String viewName) {

			super(apiClient, request, serviceId);
			
			this.viewName = viewName;
		}

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof ViewCacheLoader)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			ViewCacheLoader other = (ViewCacheLoader) obj;

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

	protected abstract static class ViewInputCacheLoader extends ServiceCacheLoader {

		protected ViewInput input;

		public ViewInputCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input) {
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
		
		private boolean compareTimeFilters(String t1, String t2) {
			
			String tu1 = TimeUtil.getTimeUnit(t1);
			String tu2 = TimeUtil.getTimeUnit(t2);
			
			if ((tu1 == null) || (tu2 == null)) {
				return t1.equals(t2);
			}
			
			int ti1 = TimeUtil.parseInterval(tu1);
			int ti2 = TimeUtil.parseInterval(tu2);

			return ti1 == ti2;
		}

		@Override
		public boolean equals(Object obj) {
		
			if (!(obj instanceof ViewInputCacheLoader)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			ViewInputCacheLoader other = (ViewInputCacheLoader) obj;
			
			if ((input.timeFilter != null) && (other.input.timeFilter != null) &&
				(!compareTimeFilters(input.timeFilter, other.input.timeFilter))) {
				return false;
			}

			if (!Objects.equal(input.view, other.input.view)) {
				return false;
			}

			Collection<String> deps = input.getDeployments(serviceId);
			Collection<String> otherDeps = other.input.getDeployments(serviceId);
			
			if (!compare(deps, otherDeps)) {
				return false;
			}
			
			Collection<String> servers = input.getServers(serviceId);
			Collection<String> otherServers = other.input.getServers(serviceId);
			
			if (!compare(servers, otherServers)) {
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

	protected abstract static class VolumeCacheLoader extends ViewInputCacheLoader {

		protected VolumeType volumeType;

		public VolumeCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input,
				VolumeType volumeType) {

			super(apiClient, request, serviceId, input);
			this.volumeType = volumeType;
		}

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof VolumeCacheLoader)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			VolumeCacheLoader other = (VolumeCacheLoader) obj;
			
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
	
	protected static class SlimVolumeCacheLoader extends VolumeCacheLoader {
		
		public SlimVolumeCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input,
				VolumeType volumeType) {

			super(apiClient, request, serviceId, input, volumeType);
		}
		
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof SlimVolumeCacheLoader)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			return true;
		}
	}


	protected static class EventsCacheLoader extends VolumeCacheLoader {
		
		public EventsCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input,
				VolumeType volumeType) {

			super(apiClient, request, serviceId, input, volumeType);
		}
		
		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof EventsCacheLoader)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			return true;
		}
	}

	protected static class GraphCacheLoader extends VolumeCacheLoader {
		protected int pointsWanted;
		protected int activeWindow;
		protected int baselineWindow;
		protected int windowSlice;

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof GraphCacheLoader)) {
				return false;
			}

			if (!super.equals( obj)) {
				return false;
			}

			GraphCacheLoader other = (GraphCacheLoader) obj;

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

		public GraphCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input,
				VolumeType volumeType, int pointsWanted, int baselineWindow, int activeWindow, int windowSlice) {

			super(apiClient, request, serviceId, input, volumeType);
			this.pointsWanted = pointsWanted;
			this.activeWindow = activeWindow;
			this.baselineWindow = baselineWindow;
			this.windowSlice = windowSlice;
		}
		
		@Override
		public String toString()
		{
			String result = super.toString() + " pw: " + pointsWanted + " aw: " 
				+ activeWindow + " bw: " + baselineWindow + " slc: " + windowSlice;
			
			return result;
		}
	}
	
	protected static class TransactionsCacheLoader extends ViewInputCacheLoader {
		protected int baselineTimespan;
		protected int activeTimespan;
		
		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof TransactionsCacheLoader)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}
			
			TransactionsCacheLoader other = (TransactionsCacheLoader)obj;
			
			if (activeTimespan != other.activeTimespan) {
				return false;
			}
			
			if (baselineTimespan != other.baselineTimespan) {
				return false;
			}

			return true;
		}

		public TransactionsCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input) {
			this(apiClient, request, serviceId, input, 0, 0);
		}
		
		public TransactionsCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId, ViewInput input, int baselineTimespan, int activeTimespan) {
			super(apiClient, request, serviceId, input);
			
			this.baselineTimespan = baselineTimespan;
			this.activeTimespan = activeTimespan;
		}
	}

	protected static class TransactionsGraphCacheLoader extends ViewInputCacheLoader {

		protected int pointsWanted;
		protected int baselineTimespan;
		protected int activeTimespan;

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof TransactionsGraphCacheLoader)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			TransactionsGraphCacheLoader other = (TransactionsGraphCacheLoader) obj;

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

		public TransactionsGraphCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId,
				ViewInput input, int pointsWanted) {
			this(apiClient, request, serviceId, input, pointsWanted, 0, 0);

		}

		
		public TransactionsGraphCacheLoader(ApiClient apiClient, ApiGetRequest<?> request, String serviceId,
				ViewInput input, int pointsWanted, int baselineTimespan, int activeTimespan) {

			super(apiClient, request, serviceId, input);
			this.pointsWanted = pointsWanted;
			this.activeTimespan = activeTimespan;
			this.baselineTimespan = baselineTimespan;
		}
	}
	
	private static class RegresionWindowCacheLoader {
		protected RegressionInput input;
		protected ApiClient apiClient;

		protected RegresionWindowCacheLoader(ApiClient apiClient, RegressionInput input) {
			this.input = input;
			this.apiClient = apiClient;
		}

		@Override
		public boolean equals(Object obj) {

			RegresionWindowCacheLoader other = (RegresionWindowCacheLoader) obj;

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
	
	protected static class RegressionCacheLoader extends EventsCacheLoader {

		protected boolean newOnly;
		protected RegressionFunction function;

		@Override
		public boolean equals(Object obj) {

			if (!(obj instanceof RegressionCacheLoader)) {
				return false;
			}

			if (!super.equals(obj)) {
				return false;
			}

			RegressionCacheLoader other = (RegressionCacheLoader) obj;
			
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
			
			if (!Objects.equal(eventInput.eventLocations, otherInput.eventLocations)) {
				return false;
			}
			
			if (!newOnly != other.newOnly) {
				return false;
			}

			return true;
		}

		public RegressionCacheLoader(ApiClient apiClient, String serviceId, ViewInput input,
				RegressionFunction function, boolean newOnly) {

			super(apiClient, null, serviceId, input, null);
			this.function = function;
			this.newOnly = newOnly;
		}
	}

	private static Response<?> getItem(BaseCacheLoader key) {
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

		ViewCacheLoader cacheKey = new ViewCacheLoader(apiClient, viewsRequest, serviceId, viewName);
		Response<ViewsResult> response = (Response<ViewsResult>)getItem(cacheKey);

		return response;
	}
	
	@SuppressWarnings("unchecked")
	public static Response<DeploymentsResult> getDeployments(ApiClient apiClient, String serviceId, boolean active) {

		DeploymentsRequest request = DeploymentsRequest.newBuilder().setServiceId(serviceId).setActive(active).build();
		DeploymentsCacheLoader cacheKey = new DeploymentsCacheLoader(apiClient, request, serviceId, active);
		Response<DeploymentsResult> response = (Response<DeploymentsResult>)getItem(cacheKey);

		return response;
	}
	
	@SuppressWarnings("unchecked")
	public static Response<ApplicationsResult> getApplications(ApiClient apiClient, String serviceId, boolean active) {

		ApplicationsRequest request = ApplicationsRequest.newBuilder().setServiceId(serviceId).setActive(active).build();
		ApplicationsCacheLoader cacheKey = new ApplicationsCacheLoader(apiClient, request, serviceId, active);
		Response<ApplicationsResult> response = (Response<ApplicationsResult>)getItem(cacheKey);

		return response;
	}
	
	public static Collection<String> getApplicationNames(ApiClient apiClient, String serviceId, boolean active) {

		Response<ApplicationsResult> response = getApplications(apiClient, serviceId, active);
		
		List<String> result = new ArrayList<String>();
		
		if ((response.data != null) && (response.data.applications != null)) {
			for (SummarizedApplication app : response.data.applications) {
				result.add(app.name);
			}
		}
		
		return result;
	}
	
	public static Response<TransactionsVolumeResult> getTransactionsVolume(ApiClient apiClient, String serviceId,
			ViewInput input, TransactionsVolumeRequest request) {
		return getTransactionsVolume(apiClient, serviceId, input, 0, 0, request);
	}

	@SuppressWarnings("unchecked")
	public static Response<TransactionsVolumeResult> getTransactionsVolume(ApiClient apiClient, String serviceId,
			ViewInput input, int activeTimespan, int baselineTimespan, TransactionsVolumeRequest request) {

		TransactionsCacheLoader cacheKey = new TransactionsCacheLoader(apiClient, request, serviceId, input, activeTimespan, baselineTimespan);
		Response<TransactionsVolumeResult> response = (Response<TransactionsVolumeResult>)getItem(cacheKey);

		return response;
	}

	@SuppressWarnings("unchecked")
	public static Response<GraphResult> getEventGraph(ApiClient apiClient, String serviceId,
			ViewInput input, VolumeType volumeType, GraphRequest request, int pointsWanted,
			int baselineWindow, int activeWindow, int windowSlice) {

		GraphCacheLoader cacheKey = new GraphCacheLoader(apiClient, request, serviceId, input, volumeType, 
			pointsWanted, baselineWindow, activeWindow, windowSlice);
		Response<GraphResult> response = (Response<GraphResult>) getItem(cacheKey);

		return response;
	}
	
	public static void putEventGraph(ApiClient apiClient, String serviceId,
			ViewInput input, VolumeType volumeType, GraphRequest request, int pointsWanted,
			int baselineWindow, int activeWindow, int windowSlice, Response<GraphResult> graphResult) {

		GraphCacheLoader cacheKey = new GraphCacheLoader(apiClient, request, serviceId, input, volumeType, 
			pointsWanted, baselineWindow, activeWindow, windowSlice);
		
		queryCache.put(cacheKey, graphResult);
	}

	@SuppressWarnings("unchecked")
	public static Response<EventsSlimVolumeResult> getEventVolume(ApiClient apiClient, String serviceId, 
			ViewInput input, VolumeType volumeType, EventsSlimVolumeRequest request) {

		SlimVolumeCacheLoader cacheKey = new SlimVolumeCacheLoader(apiClient, request, serviceId, input, volumeType);
		Response<EventsSlimVolumeResult> response = (Response<EventsSlimVolumeResult>)getItem(cacheKey);
		return response;
	}

	private static Response<?> getEventList(ApiClient apiClient, String serviceId, 
			ViewInput input, EventsRequest request, VolumeType volumeType, boolean load) {
		
		EventsCacheLoader cacheKey = new EventsCacheLoader(apiClient, request, serviceId, input, volumeType);		
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
			
			if ((response != null)  && (response.data != null)) {
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
			BaseEventVolumeInput input, int pointsWanted, int baselineTimespan, int activeTimespan, 
			TransactionsGraphRequest request) {

		TransactionsGraphCacheLoader cacheKey = new TransactionsGraphCacheLoader(apiClient, request, serviceId, input,
				pointsWanted, baselineTimespan, activeTimespan);
		Response<TransactionsGraphResult> response = (Response<TransactionsGraphResult>) ApiCache.getItem(cacheKey);
		
		return response;
	}
	
	@SuppressWarnings("unchecked")
	public static Response<EventResult> getEvent(ApiClient apiClient, String serviceId,
			String Id) {

		EventRequest.Builder builder = EventRequest.newBuilder().setServiceId(serviceId).setEventId(Id);
		
		EventCacheLoader cacheKey = new EventCacheLoader(apiClient, builder.build(), serviceId, Id);
		Response<EventResult> response = (Response<EventResult>) ApiCache.getItem(cacheKey);
		
		return response;
	}

	public static RegressionWindow getRegressionWindow(ApiClient apiClient, RegressionInput input) {

		try {
			return regressionWindowCache.get(new RegresionWindowCacheLoader(apiClient, input));

		} catch (ExecutionException e) {
			throw new IllegalStateException(e);
		}
	}
	
	public static RegressionOutput getRegressionOutput(ApiClient apiClient, String serviceId, 
		EventFilterInput input, RegressionFunction function, boolean newOnly, boolean load) {
			
		RegressionCacheLoader key = new RegressionCacheLoader(apiClient, serviceId, input, function, newOnly);
		
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
	
	private static final LoadingCache<RegressionCacheLoader, RegressionOutput> rgressionReportRache = CacheBuilder
			.newBuilder().maximumSize(CACHE_SIZE).expireAfterWrite(CACHE_REFRESH_RETENTION, TimeUnit.MINUTES)
			.build(new CacheLoader<RegressionCacheLoader, RegressionOutput>() {
				
				@Override
				public RegressionOutput load(RegressionCacheLoader key) {
					return key.function.executeRegression(key.serviceId, 
						(BaseEventVolumeInput)key.input, key.newOnly);
				}
			});

	private static final LoadingCache<RegresionWindowCacheLoader, RegressionWindow> regressionWindowCache = CacheBuilder
			.newBuilder().maximumSize(CACHE_SIZE)
			.expireAfterAccess(CACHE_REFRESH_RETENTION, TimeUnit.MINUTES)
			.refreshAfterWrite(CACHE_RELOAD_WINDOW, TimeUnit.MINUTES)
			.build(new CacheLoader<RegresionWindowCacheLoader, RegressionWindow>() {
				
				@Override
				public RegressionWindow load(RegresionWindowCacheLoader key) {
					
					RegressionWindow result = RegressionUtil.getActiveWindow(key.apiClient, key.input, 
							System.out);
					return result;
				}
			});

	private static final LoadingCache<BaseCacheLoader, Response<?>> queryCache = CacheBuilder.newBuilder()
			.maximumSize(CACHE_SIZE)
			.expireAfterAccess(CACHE_REFRESH_RETENTION, TimeUnit.MINUTES)
			.refreshAfterWrite(CACHE_REFRESH_RETENTION, TimeUnit.MINUTES)
			.build(new CacheLoader<BaseCacheLoader, Response<?>>() {
				
				@Override
				public Response<?> load(BaseCacheLoader key) {
					
					Response<?> result = key.load();
					return result;
				}
				
				@Override
				public ListenableFuture<Response<?>> reload(final BaseCacheLoader key, Response<?> prev) {
		              
					ListenableFutureTask<Response<?>> task = ListenableFutureTask.create(new Callable<Response<?>>() {
		                
						@Override
						public Response<?> call() {
		                     return key.load();
		                   }
		                });
		                 
		                Executor executor = GrafanaThreadPool.getQueryExecutor(key.apiClient);
		                executor.execute(task);
		                
		                return task;
		               
		             }
			});

}
