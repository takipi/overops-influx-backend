package com.takipi.integrations.grafana.settings;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.service.SummarizedService;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.settings.input.ServiceSettingsData;

public class GrafanaSettings {
	public static final String OO_AS_INFLUX = "oo-as-influx";
	
	public static final String EXTENSION = ".json";
	public static final String DEFAULT = File.separator + "settings" + File.separator + "oo_as_influx_default_settings" + EXTENSION;
	public static final String DEFAULT_WITHOUT_COMMENTS = File.separator + "settings" + File.separator + "oo_as_influx_default_settings_no_comments" + EXTENSION;
	
	private static final int CACHE_SIZE = 1000;
	private static final int CACHE_RETENTION = 1;
	
	private static LoadingCache<SettingsCacheKey, ServiceSettings> settingsCache = null;
	private static SettingsStorage settingsStorage = null;
	
	private static Object bundledSettingsLock = new Object();
	private static Pair<ServiceSettingsData, String> bundledSettingsData ;
	
	protected static class SettingsCacheKey {
		
		protected ApiClient apiClient;
		protected String serviceId;
		
		protected SettingsCacheKey(ApiClient apiClient, String serviceId) {
			this.apiClient = apiClient;
			this.serviceId = serviceId;
		}
		
		@Override
		public boolean equals(Object obj) {
			
			if (!(obj instanceof SettingsCacheKey)) {
				return false;
			}
			
			SettingsCacheKey other = (SettingsCacheKey)obj;
			
			if (!Objects.equal(serviceId, other.serviceId)) {
				return false;
			}
			
			if (!Objects.equal(apiClient, other.apiClient)) {
				return false;
			}
			
			return true;		
		}
		
		@Override
		public int hashCode() {
			return apiClient.hashCode() ^ serviceId.hashCode();
		}
	}
	
	private static void authService(ApiClient apiClient, String serviceId) {
		
		if (apiClient.getHostname() == "null") { // TODO: this is ugly, but it means we are authenticated elsewhere
			return;
		}
		
		List<SummarizedService> summarizedServices = ClientUtil.getEnvironments(apiClient);
		
		for (SummarizedService summarizedService : summarizedServices) {
			if (summarizedService.id.equals(serviceId)) {
				return;
			}
		}
		
		throw new IllegalStateException("Could not auth " + serviceId + " for api token");
	}
	
	private static String getServiceJsonName(String serviceId) {
		 return serviceId + EXTENSION;
	}
	
	private static Pair<ServiceSettingsData, String> getBundledDefaultSettings() {
		
		if (bundledSettingsData != null) {
			return bundledSettingsData;
		}
		
		String json;
		
		synchronized (bundledSettingsLock)
		{
			if (bundledSettingsData != null) {
				return bundledSettingsData;
			}
			
			try {
				InputStream stream = GrafanaSettings.class.getResourceAsStream(DEFAULT);

				if (stream == null) {
					return null;
				}

				json = IOUtils.toString(stream, Charset.defaultCharset());
				stream.close();
				
				ServiceSettingsData settingsData = parseServiceSettings(json);
				
				bundledSettingsData = Pair.of(settingsData, json);
				
				checkNonNullSetting(settingsData.general, "general");
				checkNonNullSetting(settingsData.regression, "regression");
				checkNonNullSetting(settingsData.slowdown, "slowdown");
				checkNonNullSetting(settingsData.regression_report, "regression_report");
				checkNonNullSetting(settingsData.cost_calculator, "cost_calculator");
				
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		
		return bundledSettingsData;	
	}
	
	private static void checkNonNullSetting(Object value, String name) {
		if (value == null) {
			throw new IllegalStateException("Missing default setting " + name);
		}
	}
	
	public static void init(SettingsStorage newSettingsStorage)
	{
		settingsStorage = newSettingsStorage;
		
		initCache();
	}
	
	private static void initCache()
	{
		settingsCache = CacheBuilder.newBuilder()
				.maximumSize(CACHE_SIZE).expireAfterWrite(CACHE_RETENTION, TimeUnit.MINUTES)
				.build(new CacheLoader<SettingsCacheKey, ServiceSettings>() {
					
					@Override
					public ServiceSettings load(SettingsCacheKey key) {
						authService(key.apiClient, key.serviceId);
						
						String name = getServiceJsonName(key.serviceId);
						String json = settingsStorage.getServiceSettings(name);
						
						if (json == null) {
							json = settingsStorage.getDefaultServiceSettings();
						}
						
						ServiceSettings result;
						
						if (json != null) {
							result = parseServiceSettings(key.serviceId, key.apiClient, json);
						} else {
							Pair<ServiceSettingsData, String> bundledSettings = getBundledDefaultSettings();
														
							if (bundledSettings != null) {
								result = getServiceSettings(key.serviceId, key.apiClient,
									bundledSettings.getFirst(), bundledSettings.getSecond());
							} else {
								result = null;
							}
						}
						
						if (result == null) {
							throw new IllegalStateException("Could not acquire settings for " + key.serviceId);
						}
						
						validateSettings(result.getData());
						
						return result;
					}
				});
	}
	
	private static void validateSettings(ServiceSettingsData serviceSettingsData) {
		
		Pair<ServiceSettingsData, String> bundledSettings = getBundledDefaultSettings();
		
		if (bundledSettings == null) {
			return;
		}
		
		ServiceSettingsData settingsData = bundledSettings.getFirst();
		
		if (serviceSettingsData.general == null) {
			serviceSettingsData.general = settingsData.general;
		}
		
		if (serviceSettingsData.general.transaction_points_wanted <= 0) {
			serviceSettingsData.general.transaction_points_wanted = settingsData.general.transaction_points_wanted;
		}
		
		if (serviceSettingsData.general.points_wanted <= 0) {
			serviceSettingsData.general.points_wanted = settingsData.general.points_wanted;
		}
		
		if (serviceSettingsData.regression == null) {
			serviceSettingsData.regression = settingsData.regression;
		}
		
		if (serviceSettingsData.regression.baseline_timespan_factor <= 0) {
			serviceSettingsData.regression.baseline_timespan_factor = settingsData.regression.baseline_timespan_factor;
		}
		
		if (serviceSettingsData.regression.min_baseline_timespan <= 0) {
			serviceSettingsData.regression.min_baseline_timespan = settingsData.regression.min_baseline_timespan;
		}
		
		if (serviceSettingsData.slowdown == null) {
			serviceSettingsData.slowdown = settingsData.slowdown;
		}
		
		if (serviceSettingsData.regression_report == null) {
			serviceSettingsData.regression_report = settingsData.regression_report;
		}
		
		if (serviceSettingsData.cost_calculator == null) {
			serviceSettingsData.cost_calculator = settingsData.cost_calculator;
		}
	}
	
	private static ServiceSettingsData parseServiceSettings(String json) {
		return new Gson().fromJson(json, ServiceSettingsData.class);
	}
	
	private static ServiceSettings getServiceSettings(String serviceId, ApiClient apiClient, 
		ServiceSettingsData data, String json) {
		return new ServiceSettings(serviceId, apiClient, json, data);
	}
	
	private static ServiceSettings parseServiceSettings(String serviceId, ApiClient apiClient, String json) {
		ServiceSettingsData data = parseServiceSettings(json);
		return new ServiceSettings(serviceId, apiClient, json, data);
	}
	
	public static ServiceSettingsData getData(ApiClient apiClient, String serviceId) {
		return getServiceSettings(apiClient, serviceId).getData(); 
	}
	
	public static ServiceSettings getServiceSettings(ApiClient apiClient, String serviceId) {
		ServiceSettings result;
		
		try {
			result = settingsCache.get(new SettingsCacheKey(apiClient, serviceId));
		} catch (ExecutionException e) {
			throw new IllegalStateException(e);
		}		
		
		return result;
	}
	
	public static String getServiceSettingsJson(ApiClient apiClient, String serviceId) {
		ServiceSettings settings = getServiceSettings(apiClient, serviceId);
		return settings.getJson();
	}

	public static void saveServiceSettings(ApiClient apiClient, String serviceId, String json) {
		
		ServiceSettings settings =  parseServiceSettings(serviceId, apiClient, json);
		
		SettingsCacheKey key = new SettingsCacheKey(apiClient, serviceId);
		
		settingsCache.put(key, settings);
		settingsStorage.saveServiceSettings(getServiceJsonName(serviceId), json);
	}
	
	public void setSettingsStorage(SettingsStorage settingsStorage) {
		if (settingsStorage == null) {
			throw new IllegalArgumentException("settingsStorage");
		}
		
		GrafanaSettings.settingsStorage = settingsStorage;
	}
}
