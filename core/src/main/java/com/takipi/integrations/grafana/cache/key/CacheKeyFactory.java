package com.takipi.integrations.grafana.cache.key;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.request.application.ApplicationsRequest;
import com.takipi.api.client.request.deployment.DeploymentsRequest;
import com.takipi.api.client.request.event.EventRequest;
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
		
		return null;
	}
}
