package com.takipi.integrations.grafana.util;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.integrations.grafana.functions.RegressionFunction;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.input.BaseEventVolumeInput;
import com.takipi.integrations.grafana.input.EventFilterInput;
import com.takipi.integrations.grafana.input.ViewInput;

public class RegressionCache
{
	private static final Logger logger = LoggerFactory.getLogger(RegressionCache.class);
	
	private static final int CACHE_SIZE = 1000;
	private static final int CACHE_REFRESH_RETENTION = 2;
	private static final int CACHE_RELOAD_WINDOW = 3;
	
	private static final LoadingCache<RegressionCacheLoader, RegressionOutput> rgressionReportRache =
			CacheBuilder.newBuilder()
				.maximumSize(CACHE_SIZE)
				.expireAfterWrite(CACHE_REFRESH_RETENTION, TimeUnit.MINUTES)
				.build(new CacheLoader<RegressionCacheLoader, RegressionOutput>()
				{
					@Override
					public RegressionOutput load(RegressionCacheLoader key)
					{
						return key.function.executeRegression(key.serviceId, (BaseEventVolumeInput)key.input, key.newOnly);
					}
				});
	
	private static final LoadingCache<RegresionWindowCacheLoader, RegressionWindow> regressionWindowCache =
			CacheBuilder.newBuilder()
				.maximumSize(CACHE_SIZE)
				.expireAfterAccess(CACHE_REFRESH_RETENTION, TimeUnit.MINUTES)
				.refreshAfterWrite(CACHE_RELOAD_WINDOW, TimeUnit.MINUTES)
				.build(new CacheLoader<RegresionWindowCacheLoader, RegressionWindow>()
				{
					@Override
					public RegressionWindow load(RegresionWindowCacheLoader key)
					{
						RegressionWindow result = RegressionUtil.getActiveWindow(key.apiClient, key.input, System.out);
						
						return result;
					}
				});
	
	protected static class RegressionCacheLoader
	{
		private final ApiClient apiClient;
		private final String serviceId;
		private final ViewInput input;
		private final boolean newOnly;
		private RegressionFunction function;
		
		public RegressionCacheLoader(ApiClient apiClient, String serviceId, ViewInput input, RegressionFunction function, boolean newOnly)
		{
			this.apiClient = apiClient;
			this.serviceId = serviceId;
			this.input = input;
			this.function = function;
			this.newOnly = newOnly;
		}
		
		@Override
		public boolean equals(Object obj)
		{
			if (!(obj instanceof RegressionCacheLoader))
			{
				return false;
			}
			
			RegressionCacheLoader other = (RegressionCacheLoader) obj;
			
			if (!Objects.equal(apiClient, other.apiClient))
			{
				return false;
			}
			
			if (!Objects.equal(serviceId, other.serviceId))
			{
				return false;
			}
			
			if ((input.timeFilter != null) &&
				(other.input.timeFilter != null) &&
				(!compareTimeFilters(input.timeFilter, other.input.timeFilter)))
			{
				return false;
			}
			
			if (!Objects.equal(input.view, other.input.view))
			{
				return false;
			}
			
			Collection<String> deps = input.getDeployments(serviceId);
			Collection<String> otherDeps = other.input.getDeployments(serviceId);
			
			if (!compareCollections(deps, otherDeps))
			{
				return false;
			}
			
			Collection<String> servers = input.getServers(serviceId);
			Collection<String> otherServers = other.input.getServers(serviceId);
			
			if (!compareCollections(servers, otherServers))
			{
				return false;
			}
			
			Collection<String> apps = input.getApplications(apiClient, serviceId);
			Collection<String> otherApps = other.input.getApplications(apiClient, serviceId);
			
			if (!compareCollections(apps, otherApps))
			{
				return false;
			}
			
			EventFilterInput eventInput = (EventFilterInput)input;
			EventFilterInput otherInput = (EventFilterInput)(other.input);
			
			if (!Objects.equal(eventInput.types, otherInput.types))
			{
				return false;
			}
						
			if (!Objects.equal(eventInput.hasTransactions(), otherInput.hasTransactions()))
			{
				return false;
			}
			
			if ((eventInput.hasTransactions()) &&
				(!Objects.equal(eventInput.transactions, otherInput.transactions)))
			{
				return false;
			}
			
			if (!Objects.equal(eventInput.searchText, otherInput.searchText))
			{
				return false;
			}
			
			if (!Objects.equal(eventInput.eventLocations, otherInput.eventLocations))
			{
				return false;
			}
			
			if (!newOnly != other.newOnly)
			{
				return false;
			}
			
			return true;
		}
		
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
		
		private boolean compareTimeFilters(String t1, String t2)
		{
			String tu1 = TimeUtil.getTimeUnit(t1);
			String tu2 = TimeUtil.getTimeUnit(t2);
			
			if ((tu1 == null) ||
				(tu2 == null))
			{
				return t1.equals(t2);
			}
			
			int ti1 = TimeUtil.parseInterval(tu1);
			int ti2 = TimeUtil.parseInterval(tu2);
			
			boolean result = (ti1 == ti2);
			
			return result;
		}
		
		@Override
		public int hashCode()
		{
			int baseHashCode = apiClient.getHostname().hashCode() ^ serviceId.hashCode();
			
			if (input.view == null)
			{
				return baseHashCode;
			}
			
			return baseHashCode ^ input.view.hashCode();
		}
		
		@Override
		public String toString()
		{
			return this.getClass().getSimpleName() + ": " + serviceId + " " + input.view + " D: " + input.deployments
					+ " A: " + input.applications + " S: " + input.servers;
		}
	}
	
	private static class RegresionWindowCacheLoader
	{
		private final RegressionInput input;
		private final ApiClient apiClient;
		
		protected RegresionWindowCacheLoader(ApiClient apiClient, RegressionInput input)
		{
			this.input = input;
			this.apiClient = apiClient;
		}
		
		@Override
		public boolean equals(Object obj)
		{
			RegresionWindowCacheLoader other = (RegresionWindowCacheLoader) obj;
			
			if (!apiClient.getHostname().equals(other.apiClient.getHostname()))
			{
				return false;
			}
			
			if (!input.serviceId.equals(other.input.serviceId))
			{
				return false;
			}
			
			if (!input.viewId.equals(other.input.viewId))
			{
				return false;
			}
			
			if (input.activeTimespan != other.input.activeTimespan)
			{
				return false;
			}
			
			if (input.baselineTimespan != other.input.baselineTimespan)
			{
				return false;
			}
			
			if ((input.deployments == null) != (other.input.deployments == null))
			{
				return false;
			}
			
			if (input.deployments != null)
			{
				if (input.deployments.size() != other.input.deployments.size())
				{
					return false;
				}
				
				for (String dep : input.deployments)
				{
					if (!other.input.deployments.contains(dep))
					{
						return false;
					}
				}
			}
			
			return true;
		}
		
		@Override
		public int hashCode()
		{
			StringBuilder result = new StringBuilder();
			
			result.append(input.serviceId).append(input.viewId);
			
			if (input.deployments != null)
			{
				for (String dep : input.deployments)
				{
					result.append(dep);
				}
			}
			
			return result.toString().hashCode();
		}
	}
	
	public static RegressionWindow getRegressionWindow(ApiClient apiClient, RegressionInput input)
	{
		try
		{
			return regressionWindowCache.get(new RegresionWindowCacheLoader(apiClient, input));
		}
		catch (ExecutionException e)
		{
			throw new IllegalStateException(e);
		}
	}
	
	public static RegressionOutput getRegressionOutput(ApiClient apiClient,
			String serviceId, EventFilterInput input, RegressionFunction function, boolean newOnly, boolean load)
	{
		RegressionCacheLoader key = new RegressionCacheLoader(apiClient, serviceId, input, function, newOnly);
		
		if (load)
		{
			try
			{
				RegressionOutput result = rgressionReportRache.get(key);
				
				if (result.empty)
				{
					rgressionReportRache.invalidate(key);
				}
				
				return result;
			}
			catch (ExecutionException e)
			{
				throw new IllegalStateException(e);
			}
		}
		else
		{
			return rgressionReportRache.getIfPresent(key);
		}
	}
}
