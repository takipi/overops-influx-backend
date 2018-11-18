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
import com.google.gson.GsonBuilder;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.service.SummarizedService;
import com.takipi.api.client.util.client.ClientUtil;

public class GrafanaSettings {

	public static final String SETTINGS = File.separator + "settings" + File.separator;
	public static final String EXTENSION = ".json";
	public static final String DEFAULT = "default" + EXTENSION;
	
	private static final int CACHE_SIZE = 1000;
	private static final int CACHE_RETENTION = 1;
	
	private static LoadingCache<SettingsCacheKey, ServiceSettings> settingsCache = null;
	private static SettingsStorage settingsStorage = null;
	
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
	
	private static String getBundledDefaultSettings() {
		try {
			InputStream stream = GrafanaSettings.class.getResourceAsStream(SETTINGS + DEFAULT);

			if (stream == null) {
				return null;
			}

			String result = IOUtils.toString(stream, Charset.defaultCharset());
			stream.close();
			
			return result;
		} catch (Exception e) {
			throw new IllegalStateException(e);
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
						
						if (json == null) {
							json = getBundledDefaultSettings();
						}
						
						if (json == null) {
							throw new IllegalStateException("Could not acquire settings for " + key.serviceId);
						}
						
						Gson gson = new Gson();
						ServiceSettings result = (ServiceSettings)gson.fromJson(json, ServiceSettings.class);

						return result;
					}
				});
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
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String result = gson.toJson(settings);
		
		return result;
	}

	public static void saveServiceSettings(ApiClient apiClient, String serviceId, String json) {
		ServiceSettings serviceSettings = (ServiceSettings)(new Gson().fromJson(json, ServiceSettings.class));
		
		saveServiceSettings(apiClient, serviceId, serviceSettings);
	}
	
	public static void saveServiceSettings(ApiClient apiClient, String serviceId, ServiceSettings serviceSettings) {
		authService(apiClient, serviceId);
		
		Gson gson = new Gson();
		String json = gson.toJson(serviceSettings);
		
		SettingsCacheKey key = new SettingsCacheKey(apiClient, serviceId);
		
		settingsCache.put(key, serviceSettings);
		settingsStorage.saveServiceSettings(getServiceJsonName(serviceId), json);
	}
	
	public void setSettingsStorage(SettingsStorage settingsStorage) {
		if (settingsStorage == null) {
			throw new IllegalArgumentException("settingsStorage");
		}
		
		GrafanaSettings.settingsStorage = settingsStorage;
	}
}
