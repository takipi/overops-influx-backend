package com.takipi.integrations.grafana.functions;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.event.Location;
import com.takipi.common.api.data.service.SummarizedService;
import com.takipi.common.api.data.view.SummarizedView;
import com.takipi.common.api.request.TimeframeRequest;
import com.takipi.common.api.request.event.EventsVolumeRequest;
import com.takipi.common.api.result.event.EventResult;
import com.takipi.common.api.result.event.EventsVolumeResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.common.api.util.Pair;
import com.takipi.common.api.util.ValidationUtil.VolumeType;
import com.takipi.common.udf.util.ApiFilterUtil;
import com.takipi.common.udf.util.ApiViewUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.FilterInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;

public abstract class GrafanaFunction {

	public interface FunctionFactory {
		public GrafanaFunction create(ApiClient apiClient); 
		public Class<?> getInputClass();
		public String getName();
	}
	
	protected static final String SERIES_NAME = "events";
	protected static final String EMPTY_NAME = "";
	
	protected static final String SUM_COLUMN = "sum";
	protected static final String TIME_COLUMN = "time";
	protected static final String KEY_COLUMN = "key";
	protected static final String VALUE_COLUMN = "value";
	
	public static final String GRAFANA_SEPERATOR = Pattern.quote("|");
	public static final String ARRAY_SEPERATOR = Pattern.quote(",");
	public static final String SERVICE_SEPERATOR = ": ";
	public static final String VAR_ALL = "*";

	protected final ApiClient apiClient;
	
	protected static final Executor executor = Executors.newFixedThreadPool(10);

	public GrafanaFunction(ApiClient apiClient) {
		this.apiClient = apiClient;
	}
	
	public static String getSimpleClassName(String className) {
		String qualified = className.replace('/', '.'); 
		int sepIdex = Math.max(qualified.lastIndexOf('.') + 1, 0);
		String result = qualified.substring(sepIdex, qualified.length());
		return result;
	}
	
	protected void validateResponse(Response<?> response){
		if ((response.isBadResponse()) || (response.data == null)) {
			throw new IllegalStateException("EventsResult code " + response.responseCode);
		}

	}
	protected static String formatLocation(Location location) {
		return getSimpleClassName(location.class_name) + "." + location.method_name;
	}
	
	protected String[] getServiceIds (EnvironmentsInput input) {
		String[] serviceIds = input.getServiceIds();
		
		if (serviceIds.length != 0) {
			return serviceIds;
		}
		
		List<SummarizedService> services = ApiFilterUtil.getEnvironments(apiClient);
		
		String[] result = new String[services.size()];
		
		for (int i = 0; i < services.size(); i++) {
			result[i] = services.get(i).id;
		}
		
		return result;
	}
		
	protected List<EventResult> getEventList(String serviceId, ViewInput request,
			Pair<String, String> timeSpan) {

		String viewId = getViewId(serviceId, request.view);
		
		if (viewId == null) {
			return null;
		}

		EventsVolumeRequest.Builder builder = EventsVolumeRequest.newBuilder().setServiceId(serviceId).setFrom(timeSpan.getFirst())
				.setTo(timeSpan.getSecond()).setViewId(viewId).setVolumeType(VolumeType.all);

		applyFilters(request, serviceId, builder);

		Response<EventsVolumeResult> response = apiClient.get(builder.build());

		validateResponse(response);
		
		if (response.data == null) {
			return Collections.emptyList();
		}
		
		return response.data.events;
	}

	public static void applyFilters(FilterInput request, String serviceId, TimeframeRequest.Builder builder) {
		
		for (String app : request.getApplications(serviceId)) {
			builder.addApp(app);
		}

		for (String dep : request.getDeployments(serviceId)) {
			builder.addDeployment(dep);
		}

		for (String server : request.getServers(serviceId)) {
			builder.addServer(server);
		}
	}
	
	protected SummarizedView getView(String serviceId, String viewName) {
		SummarizedView view = ApiViewUtil.getServiceViewByName(apiClient, serviceId, viewName);
		return view;
	}

	protected String getViewId(String serviceId, String viewName) {
		SummarizedView view = getView(serviceId, viewName);

		if (view == null) {
			return null;
		}

		return view.id;
	}
	
	public abstract  List<Series> process(FunctionInput functionInput);

}
