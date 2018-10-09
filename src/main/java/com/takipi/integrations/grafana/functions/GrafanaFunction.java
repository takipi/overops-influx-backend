package com.takipi.integrations.grafana.functions;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.service.SummarizedService;
import com.takipi.common.api.data.view.SummarizedView;
import com.takipi.common.api.request.TimeframeRequest;
import com.takipi.common.api.request.volume.EventsVolumeRequest;
import com.takipi.common.api.result.event.EventResult;
import com.takipi.common.api.result.volume.EventsVolumeResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.common.api.util.Pair;
import com.takipi.common.api.util.ValidationUtil.VolumeType;
import com.takipi.common.udf.util.ApiFilterUtil;
import com.takipi.common.udf.util.ApiViewUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.FilterInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.output.ResultContent;
import com.takipi.integrations.grafana.output.Series;

public abstract class GrafanaFunction {

	public interface FunctionFactory {
		public GrafanaFunction create(ApiClient apiClient); 
		public Class<?> getInputClass();
		public String getName();
	}
	
	protected static final String SERIES_NAME = "events";
	protected static final String SUM_COLUMN = "sum";
	protected static final String TIME_COLUMN = "time";
	protected static final String KEY_COLUMN = "key";
	protected static final String VALUE_COLUMN = "value";
	
	public static final String GRAFANA_SEPERATOR = Pattern.quote("|");
	public static final String ARRAY_SEPERATOR = Pattern.quote(",");
	public static final String SERVICE_SEPERATOR = ": ";
	public static final String VAR_ALL = "*";

	private static final Map<String, FunctionFactory> factories;

	protected final ApiClient apiClient;

	protected static class EventVolume {
		protected long sum;
		protected long count;
	}
	
	public GrafanaFunction(ApiClient apiClient) {
		this.apiClient = apiClient;
	}

	protected static QueryResult createQueryResults(List<Series> series) {
		ResultContent resultContent = new ResultContent();
		resultContent.statement_id = 0;
		resultContent.series = series;
		QueryResult result = new QueryResult();
		result.results = Collections.singletonList(resultContent);

		return result;
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

		String viewId = getViewId(serviceId, request);
		
		if (viewId == null) {
			return null;
		}

		EventsVolumeRequest.Builder builder = EventsVolumeRequest.newBuilder().setServiceId(serviceId).setFrom(timeSpan.getFirst())
				.setTo(timeSpan.getSecond()).setViewId(viewId).setVolumeType(VolumeType.all);

		applyFilters(request, serviceId, builder);

		Response<EventsVolumeResult> eventsResult = apiClient.get(builder.build());

		if (eventsResult.isBadResponse()) {
			throw new IllegalStateException("EventsResult code " + eventsResult.responseCode);
		}
		
		if (eventsResult.data == null) {
			return Collections.emptyList();
		}
		
		return eventsResult.data.events;
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

	protected String getViewId(String serviceId, ViewInput request) {
		SummarizedView view = ApiViewUtil.getServiceViewByName(apiClient, serviceId, request.view);

		if (view == null) {
			throw new IllegalArgumentException("View " + request.view + " not found in " + serviceId);
		}

		return view.id;
	}

	public static QueryResult processQuery(ApiClient apiClient, String query) {
		if ((query == null) || (query.length() == 0)) {
			throw new IllegalArgumentException("Missing query");
		}

		String trimmedQuery = query.trim();

		int parenthesisIndex = trimmedQuery.indexOf('(');

		if (parenthesisIndex == -1) {
			throw new IllegalArgumentException("Missing opening parenthesis: " + query);
		}

		char endChar = trimmedQuery.charAt(trimmedQuery.length() - 1);

		if (endChar != ')') {
			throw new IllegalArgumentException("Missing closing parenthesis: " + query);
		}

		String name = trimmedQuery.substring(0, parenthesisIndex);
		String params = trimmedQuery.substring(parenthesisIndex + 1, trimmedQuery.length() - 1);

		FunctionFactory factory = factories.get(name);
		
		if (factory == null) {
			throw new IllegalArgumentException("Unsupported function " + name);
		}
	
		String json = params.replace("\\", "");
		FunctionInput input = (FunctionInput)(new Gson().fromJson(json, factory.getInputClass()));
		GrafanaFunction function = factory.create(apiClient);
		
		return function.process(input);
	}
	
	public abstract QueryResult process(FunctionInput functionInput);
	
	public static void registerFunction(FunctionFactory factory) {
		factories.put(factory.getName(), factory);
	}
	
	static {
		factories = new HashMap<String, FunctionFactory>();
		
		registerFunction(new EventsFunction.Factory());
		registerFunction(new GraphFunction.Factory());
		registerFunction(new GroupByFunction.Factory());
		registerFunction(new VolumeFunction.Factory());
		registerFunction(new CategoryFunction.Factory());
		
		registerFunction(new EnvironmentsFunction.Factory());
		registerFunction(new ApplicationsFunction.Factory());
		registerFunction(new ServersFunction.Factory());
		registerFunction(new DeploymentsFunction.Factory());
		registerFunction(new ViewsFunction.Factory());
	}
	
}
