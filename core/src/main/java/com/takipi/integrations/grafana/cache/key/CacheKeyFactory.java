package com.takipi.integrations.grafana.cache.key;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.application.ApplicationsRequest;
import com.takipi.api.client.request.deployment.DeploymentsRequest;
import com.takipi.api.client.request.event.EventRequest;
import com.takipi.api.client.request.event.EventsRequest;
import com.takipi.api.client.request.event.EventsSlimVolumeRequest;
import com.takipi.api.client.request.event.EventsVolumeRequest;
import com.takipi.api.client.request.metrics.GraphRequest;
import com.takipi.api.client.request.process.JvmsRequest;
import com.takipi.api.client.request.server.ServersRequest;
import com.takipi.api.client.request.view.ViewsRequest;
import com.takipi.api.core.request.intf.ApiGetRequest;
import com.takipi.api.core.result.intf.ApiResult;
import com.takipi.integrations.grafana.cache.key.base.CacheKey;
import com.takipi.integrations.grafana.cache.key.clients.CacheGetApplicationsKey;
import com.takipi.integrations.grafana.cache.key.clients.CacheGetDeploymentsKey;
import com.takipi.integrations.grafana.cache.key.clients.CacheGetJvmsKey;
import com.takipi.integrations.grafana.cache.key.clients.CacheGetServersKey;
import com.takipi.integrations.grafana.cache.key.events.CacheGetEventKey;
import com.takipi.integrations.grafana.cache.key.events.CacheGetEventsGraphKey;
import com.takipi.integrations.grafana.cache.key.events.CacheGetEventsKey;
import com.takipi.integrations.grafana.cache.key.events.CacheGetEventsSlimVolumeKey;
import com.takipi.integrations.grafana.cache.key.events.CacheGetEventsVolumeKey;
import com.takipi.integrations.grafana.cache.key.views.CacheGetViewKey;

public class CacheKeyFactory
{
	public static <T extends ApiResult> CacheKey getKey(ApiClient apiClient, ApiGetRequest<T> request)
	{
		if (request instanceof DeploymentsRequest)
		{
			return CacheGetDeploymentsKey.create(apiClient, (DeploymentsRequest) request);
		}
		else if (request instanceof ApplicationsRequest)
		{
			return CacheGetApplicationsKey.create(apiClient, (ApplicationsRequest) request);
		}
		else if (request instanceof ServersRequest)
		{
			return CacheGetServersKey.create(apiClient, (ServersRequest) request);
		}
		else if (request instanceof JvmsRequest)
		{
			return CacheGetJvmsKey.create(apiClient, (JvmsRequest) request);
		}
		else if (request instanceof ViewsRequest)
		{
			return CacheGetViewKey.create(apiClient, (ViewsRequest) request);
		}
		else if (request instanceof EventRequest)
		{
			return CacheGetEventKey.create(apiClient, (EventRequest) request);
		}
		else if (request instanceof EventsRequest)
		{
			// TODO
			// Smarter mechanism that tries to return a good enough result based on volume type.
			//
			return CacheGetEventsKey.create(apiClient, (EventsRequest) request);
		}
		else if (request instanceof EventsVolumeRequest)
		{
			// TODO
			// Smarter mechanism that tries to return a good enough result based on volume type.
			//
			return CacheGetEventsVolumeKey.create(apiClient, (EventsVolumeRequest) request);
		}
		else if (request instanceof EventsSlimVolumeRequest)
		{
			// TODO
			// Smarter mechanism that tries to return a good enough result based on volume type.
			//
			return CacheGetEventsSlimVolumeKey.create(apiClient, (EventsSlimVolumeRequest) request);
		}
		else if (request instanceof GraphRequest)
		{
			// TODO
			// Smarter mechanism that tries to return a good enough result based on volume type.
			//
			return CacheGetEventsGraphKey.create(apiClient, (GraphRequest) request);
		}
		
		return null;
	}
}
