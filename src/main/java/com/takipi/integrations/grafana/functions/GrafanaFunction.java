package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.event.Location;
import com.takipi.api.client.data.event.Stats;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.data.transaction.Transaction;
import com.takipi.api.client.data.view.SummarizedView;
import com.takipi.api.client.request.TimeframeRequest;
import com.takipi.api.client.request.ViewTimeframeRequest;
import com.takipi.api.client.request.event.EventsRequest;
import com.takipi.api.client.request.event.EventsVolumeRequest;
import com.takipi.api.client.request.metrics.GraphRequest;
import com.takipi.api.client.request.transaction.TransactionsVolumeRequest;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.result.event.EventsResult;
import com.takipi.api.client.result.event.EventsVolumeResult;
import com.takipi.api.client.result.metrics.GraphResult;
import com.takipi.api.client.result.transaction.TransactionsVolumeResult;
import com.takipi.api.client.util.validation.ValidationUtil.GraphType;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.api.client.util.view.ViewUtil;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.EnvironmentsFilterInput;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.ApiCache;
import com.takipi.integrations.grafana.utils.TimeUtils;

public abstract class GrafanaFunction {

	private static int MAX_COMBINE_SERVICES = 3;

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

	private static final DateTimeFormatter fmt = ISODateTimeFormat.dateTime().withZoneUTC();

	protected final ApiClient apiClient;

	public GrafanaFunction(ApiClient apiClient) {
		this.apiClient = apiClient;
	}

	public static String toQualified(String value) {
		return value.replace('/', '.');
	}

	public static String getSimpleClassName(String className) {
		String qualified = toQualified(className);
		int sepIdex = Math.max(qualified.lastIndexOf('.') + 1, 0);
		String result = qualified.substring(sepIdex, qualified.length());
		return result;
	}

	protected void validateResponse(Response<?> response) {
		if ((response.isBadResponse()) || (response.data == null)) {
			throw new IllegalStateException("EventsResult code " + response.responseCode);
		}

	}

	protected static String formatLocation(Location location) {
		return getSimpleClassName(location.class_name) + "." + location.method_name;
	}

	protected String[] getServiceIds(EnvironmentsInput input) {

		String[] serviceIds = input.getServiceIds();
		String[] result = Arrays.copyOf(serviceIds, Math.min(MAX_COMBINE_SERVICES, serviceIds.length));

		return result;
	}

	private void applyBuilder(ViewTimeframeRequest.Builder builder, String serviceId, String viewId,
			Pair<String, String> timeSpan, ViewInput request) {

		builder.setViewId(viewId).setServiceId(serviceId).setFrom(timeSpan.getFirst()).setTo(timeSpan.getSecond());

		applyFilters(request, serviceId, builder);
	}

	protected Collection<Transaction> getTransactions(String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			ViewInput input) {

		Pair<String, String> fromTo = TimeUtils.toTimespan(timeSpan);
		
		TransactionsVolumeRequest.Builder builder = TransactionsVolumeRequest.newBuilder().setServiceId(serviceId)
				.setViewId(viewId).setFrom(fromTo.getFirst()).setTo(fromTo.getSecond());

		applyFilters(input, serviceId, builder);

		Response<TransactionsVolumeResult> response = ApiCache.getTransactionsVolume(apiClient, serviceId, viewId,
				input, builder.build());

		if (response.isBadResponse()) {
			throw new IllegalStateException(
					"Transnaction volume for service " + serviceId + " code: " + response.responseCode);
		}

		if ((response.data == null) || (response.data.transactions == null)) {
			return null;
		}

		Collection<Transaction> result;

		if (input.hasTransactions()) {

			result = new ArrayList<Transaction>(response.data.transactions.size());
			Collection<String> transactions = input.getTransactions(serviceId);

			for (Transaction transaction : response.data.transactions) {

				String entryPoint = getSimpleClassName(transaction.name);

				if ((transactions != null) && (!transactions.contains(entryPoint))) {
					continue;
				}

				result.add(transaction);
			}

		} else {
			result = response.data.transactions;
		}

		return result;
	}

	protected String getSeriesName(BaseGraphInput input, String seriesName, Object volumeType, String serviceId,
			String[] serviceIds) {

		return getServiceValue(input.deployments, serviceId, serviceIds);
	}

	protected static Graph getEventsGraph(ApiClient apiClient, String serviceId, String viewId, int pointsCount,
			ViewInput input, VolumeType volumeType, DateTime from, DateTime to) {

		GraphRequest.Builder builder = GraphRequest.newBuilder().setServiceId(serviceId).setViewId(viewId)
				.setGraphType(GraphType.view).setFrom(from.toString(fmt)).setTo(to.toString(fmt))
				.setVolumeType(volumeType).setWantedPointCount(pointsCount);

		applyFilters(input, serviceId, builder);

		Response<GraphResult> graphResponse = ApiCache.getEventGraph(apiClient, serviceId, viewId, input, volumeType,
				builder.build(), pointsCount);

		if (graphResponse.isBadResponse()) {
			return null;
		}

		GraphResult graphResult = graphResponse.data;

		if (graphResult == null) {
			return null;
		}

		if (CollectionUtil.safeIsEmpty(graphResult.graphs)) {
			return null;
		}

		Graph result = graphResult.graphs.get(0);

		if (!viewId.equals(result.id)) {
			return null;
		}

		if (CollectionUtil.safeIsEmpty(result.points)) {
			return null;
		}

		return result;
	}

	private static void appendGraphStats(Map<String, EventResult> eventMap, Graph graph) {

		for (GraphPoint gp : graph.points) {

			if (gp.contributors == null) {
				continue;
			}

			for (GraphPointContributor gpc : gp.contributors) {

				EventResult event = eventMap.get(gpc.id);

				if (event == null) {
					continue;
				}

				if (event.stats == null) {
					event.stats = new Stats();
				}

				event.stats.hits += gpc.stats.hits;
				event.stats.invocations += gpc.stats.invocations;
			}
		}
	}
	
	private Collection<EventResult> getEventListFromGraph(String serviceId, String viewId, ViewInput input, DateTime from, DateTime to,
			VolumeType volumeType, int pointsCount) {
		
		GraphRequest.Builder builder = GraphRequest.newBuilder().setServiceId(serviceId).setViewId(viewId)
				.setGraphType(GraphType.view).setFrom(from.toString(fmt)).setTo(to.toString(fmt))
				.setVolumeType(volumeType).setWantedPointCount(pointsCount);

		applyFilters(input, serviceId, builder);
		
		Graph graph = getEventsGraph(apiClient, serviceId, viewId, pointsCount, input, volumeType, from, to);
		Collection<EventResult> events = getEventList(serviceId, viewId, input, from, to, volumeType);
		
		if (events == null) {
			return null;
		}
		
		Map<String, EventResult> eventsMap = getEventsMap(events);
		appendGraphStats(eventsMap, graph);
	
		return eventsMap.values();			
	}
	
	private Collection<EventResult> getEventListFromVolume(String serviceId, String viewId, ViewInput input, DateTime from, DateTime to,
			VolumeType volumeType, int pointsCount) {
		
		EventsVolumeRequest.Builder builder = EventsVolumeRequest.newBuilder().setVolumeType(volumeType);
		applyBuilder(builder, serviceId, viewId, TimeUtils.toTimespan(from, to), input);				
		Response<EventsVolumeResult> volumeResponse = ApiCache.getEventVolume(apiClient, serviceId, viewId, input, volumeType, builder.build());
		validateResponse(volumeResponse);
		
		return volumeResponse.data.events;
	}
	
	private Collection<EventResult> getEventList(String serviceId, String viewId, ViewInput input, DateTime from, DateTime to,
			VolumeType volumeType) {
		
		EventsRequest.Builder builder = EventsRequest.newBuilder();
		applyBuilder(builder, serviceId, viewId, TimeUtils.toTimespan(from, to), input);		
		Response<EventsResult> eventResponse = ApiCache.getEventList(apiClient, serviceId, viewId, input, builder.build());
		validateResponse(eventResponse);	

		return eventResponse.data.events;
	}
	
	private static Map<String, EventResult> getEventsMap(Collection< EventResult> events) {
		
		 Map<String, EventResult> result = new HashMap<String, EventResult>();
		
		for (EventResult event: events) {
			result.put(event.id, event);
		}
		
		return result;
	}

	protected Map<String, EventResult> getEventMap(String serviceId, ViewInput input, DateTime from, DateTime to,
			VolumeType volumeType, int pointsCount) {
	
		String viewId = getViewId(serviceId, input.view);

		if (viewId == null) {
			return null;
		}

		Collection<EventResult> events;
		
		Map<String, EventResult> result = new HashMap<String, EventResult>();

		if (volumeType != null) {	
			if (pointsCount > 0) {
				events = getEventListFromGraph(serviceId, viewId, input, from, to, volumeType, pointsCount);
								
			} else { 
				events = getEventListFromVolume(serviceId, viewId, input, from, to, volumeType, pointsCount);
			}		
		} else {
			events = getEventList(serviceId, viewId, input, from, to, volumeType);
		}
		
		if (events == null) {
			return null;

		}

		result = getEventsMap(events);
				
		return result;
	}

	public static void applyFilters(EnvironmentsFilterInput input, String serviceId, TimeframeRequest.Builder builder) {

		for (String app : input.getApplications(serviceId)) {
			builder.addApp(app);
		}

		for (String dep : input.getDeployments(serviceId)) {
			builder.addDeployment(dep);
		}

		for (String server : input.getServers(serviceId)) {
			builder.addServer(server);
		}
	}

	protected SummarizedView getView(String serviceId, String viewName) {
		SummarizedView view = ViewUtil.getServiceViewByName(apiClient, serviceId, viewName);
		return view;
	}

	protected static String getServiceValue(String value, String serviceId) {
		return value + SERVICE_SEPERATOR + serviceId;
	}

	protected static String getServiceValue(String value, String serviceId, String[] serviceIds) {

		if (serviceIds.length == 1) {
			return value;
		} else {
			return getServiceValue(value, serviceId);
		}
	}

	protected String getViewId(String serviceId, String viewName) {
		SummarizedView view = getView(serviceId, viewName);

		if (view == null) {
			return null;
		}

		return view.id;
	}

	public abstract List<Series> process(FunctionInput functionInput);
}
