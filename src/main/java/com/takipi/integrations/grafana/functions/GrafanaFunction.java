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
import com.takipi.api.client.data.metrics.Graph;
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
import com.takipi.integrations.grafana.input.EnvironmentsFilterInput;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.ApiCache;

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
		String[] result =  Arrays.copyOf(serviceIds, Math.min(MAX_COMBINE_SERVICES, serviceIds.length));	
		
		return result;
	}

	private void applyBuilder(ViewTimeframeRequest.Builder builder, String serviceId, String viewId,
			Pair<String, String> timeSpan, ViewInput request) {

		builder.setViewId(viewId).setServiceId(serviceId).setFrom(timeSpan.getFirst()).setTo(timeSpan.getSecond());

		applyFilters(request, serviceId, builder);
	}

	protected Collection<Transaction> getTransactions(String serviceId, String viewId, Pair<String, String> timeSpan,
			ViewInput input) {
		
		TransactionsVolumeRequest.Builder builder = TransactionsVolumeRequest.newBuilder().setServiceId(serviceId)
				.setViewId(viewId).setFrom(timeSpan.getFirst()).setTo(timeSpan.getSecond());

		applyFilters(input, serviceId, builder);

		Response<TransactionsVolumeResult> response = ApiCache.getTransactionsVolume(apiClient, serviceId, viewId, input, builder.build());
				
		
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
	
	public static int compareDeployments(Object o1, Object o2) {
		
		double i1 = getDeplyomentNumber(o1.toString());
		double i2 = getDeplyomentNumber(o2.toString());

		double d = i2 - i1;

		if (d == 0) {
			return 0;
		}

		if (d < 0) {
			return -1;
		}

		return 1;

	}
	
	private static double getDeplyomentNumber(String value) {

		boolean hasDot = false;
		boolean hasNums = false;

		StringBuilder number = new StringBuilder();

		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);

			if (c == '.') {
				if (!hasDot) {
					number.append(c);
					hasDot = true;
				}
				continue;
			}

			if ((c >= '0') && (c <= '9')) {
				number.append(c);
				hasNums = true;
			}
		}

		if (hasNums) {
			double result = Double.parseDouble(number.toString());
			return result;
		} else {
			return -1;
		}
	}
	
	protected static Graph getEventsGraph(ApiClient apiClient, String serviceId, String viewId, int pointsCount,
			ViewInput input, VolumeType volumeType, DateTime from, DateTime to) {
		
						
		GraphRequest.Builder builder = GraphRequest.newBuilder().setServiceId(serviceId).setViewId(viewId)
				.setGraphType(GraphType.view).setFrom(from.toString(fmt)).setTo(to.toString(fmt))
				.setVolumeType(volumeType).setWantedPointCount(pointsCount);

		applyFilters(input, serviceId, builder);
		
		Response<GraphResult> graphResponse = ApiCache.getEventGraph(apiClient, serviceId, viewId,
			input, volumeType, builder.build(), pointsCount);
				
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
	
	protected Collection<EventResult> getEventList(String serviceId, ViewInput input, Pair<String, String> timeSpan,
			VolumeType volumeType) {
		 
		Map<String, EventResult> result = getEventMap(serviceId, input, timeSpan, volumeType);
		 
		if (result != null) {
			return result.values();
		} else {
			return null;
		}
	}
		
	protected Map<String, EventResult> getEventMap(String serviceId, ViewInput input, Pair<String, String> timeSpan,
			VolumeType volumeType) {
	
		String viewId = getViewId(serviceId, input.view);

		if (viewId == null) {
			return null;
		}

		List<EventResult> events;
		
		Map<String, EventResult> result = new HashMap<String, EventResult>();

		if (volumeType != null) {
			
			EventsVolumeRequest.Builder builder = EventsVolumeRequest.newBuilder().setVolumeType(volumeType);
			applyBuilder(builder, serviceId, viewId, timeSpan, input);
			Response<EventsVolumeResult> volumeResponse = ApiCache.getEventVolume(apiClient, serviceId, viewId, input, volumeType, builder.build());
			validateResponse(volumeResponse);
			events = volumeResponse.data.events;
		} else {
			EventsRequest.Builder builder = EventsRequest.newBuilder();
			applyBuilder(builder, serviceId, viewId, timeSpan, input);		
			Response<EventsResult> eventResponse = ApiCache.getEventList(apiClient, serviceId, viewId, input, builder.build());
			validateResponse(eventResponse);
			events = eventResponse.data.events;
		}
		
		if (events == null) {
			return null;

		}

		for (EventResult event: events) {
			result.put(event.id, event);
		}
				
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
