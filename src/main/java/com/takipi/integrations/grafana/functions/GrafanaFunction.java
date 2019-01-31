package com.takipi.integrations.grafana.functions;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.collect.Lists;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.event.Location;
import com.takipi.api.client.data.event.Stats;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.data.transaction.Transaction;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.data.view.SummarizedView;
import com.takipi.api.client.request.TimeframeRequest;
import com.takipi.api.client.request.ViewTimeframeRequest;
import com.takipi.api.client.request.event.EventsRequest;
import com.takipi.api.client.request.event.EventsSlimVolumeRequest;
import com.takipi.api.client.request.metrics.GraphRequest;
import com.takipi.api.client.request.transaction.TransactionsGraphRequest;
import com.takipi.api.client.request.transaction.TransactionsVolumeRequest;
import com.takipi.api.client.request.view.ViewsRequest;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.result.event.EventSlimResult;
import com.takipi.api.client.result.event.EventsResult;
import com.takipi.api.client.result.event.EventsSlimVolumeResult;
import com.takipi.api.client.result.event.EventsVolumeResult;
import com.takipi.api.client.result.metrics.GraphResult;
import com.takipi.api.client.result.transaction.TransactionsGraphResult;
import com.takipi.api.client.result.transaction.TransactionsVolumeResult;
import com.takipi.api.client.result.view.ViewsResult;
import com.takipi.api.client.util.validation.ValidationUtil.GraphType;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.BaseEventVolumeInput;
import com.takipi.integrations.grafana.input.BaseGraphInput;
import com.takipi.integrations.grafana.input.EnvironmentsFilterInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings.GroupFilter;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.TimeUtil;

public abstract class GrafanaFunction
{	
	public interface FunctionFactory
	{
		public GrafanaFunction create(ApiClient apiClient);
		
		public Class<?> getInputClass();
		
		public String getName();
	}
	
	protected static final String RESOVED = "Resolved";
	protected static final String HIDDEN = "Hidden";
	
	protected static final String SERIES_NAME = "events";
	protected static final String EMPTY_NAME = "";
	
	protected static final String SUM_COLUMN = "sum";
	protected static final String TIME_COLUMN = "time";
	protected static final String KEY_COLUMN = "key";
	protected static final String VALUE_COLUMN = "value";
	
	public static final String GRAFANA_SEPERATOR_RAW = "|";
	public static final String ARRAY_SEPERATOR_RAW = ",";
	public static final String GRAFANA_VAR_ADD = "And";

	public static final String GRAFANA_SEPERATOR = Pattern.quote(GRAFANA_SEPERATOR_RAW);
	public static final String ARRAY_SEPERATOR = Pattern.quote(ARRAY_SEPERATOR_RAW);
	public static final String SERVICE_SEPERATOR = ": ";
	public static final String GRAFANA_VAR_PREFIX = "$";
	
	public static final String ALL = "All";
	public static final String NONE = "None";
	public static final List<String> VAR_ALL = Arrays.asList(new String[] { "*", ALL, 
		ALL.toLowerCase(), NONE, NONE.toLowerCase() });
	
	protected static final char QUALIFIED_DELIM = '.';
	protected static final char INTERNAL_DELIM = '/';
	protected static final String TRANS_DELIM = "#";
	protected static final String EMPTY_POSTFIX = ".";	
	
	protected static final String HTTP = "http://";
	protected static final String HTTPS = "https://";
	
	protected static final DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTime().withZoneUTC();
	protected static final DecimalFormat decimalFormat = new DecimalFormat("#.##");

	protected static final Map<String, String> TYPES_MAP;
	
	protected static final String LOGGED_ERROR = "Logged Error";
	protected static final String LOGGED_WARNING = "Logged Warning";
	protected static final String CAUGHT_EXCEPTION = "Caught Exception";
	protected static final String UNCAUGHT_EXCEPTION = "Uncaught Exception";
	protected static final String SWALLOWED_EXCEPTION = "Swallowed Exception";
	protected static final String TIMER = "Timer";
	protected static final String HTTP_ERROR = "HTTP Error";

	static
	{
		TYPES_MAP = new HashMap<String, String>();
		
		TYPES_MAP.put(LOGGED_ERROR, "ERR");
		TYPES_MAP.put(LOGGED_WARNING, "WRN");
		TYPES_MAP.put(CAUGHT_EXCEPTION, "CEX");
		TYPES_MAP.put(UNCAUGHT_EXCEPTION, "UNC");
		TYPES_MAP.put(SWALLOWED_EXCEPTION, "SWL");
		TYPES_MAP.put(TIMER, "TMR");
		TYPES_MAP.put(HTTP_ERROR, "HTTP");	
	}
	
	private static final int END_SLICE_POINT_COUNT = 2;
	private static final int NO_GRAPH_SLICE = -1;
	
	protected final ApiClient apiClient;
	
	protected class GraphSliceTaskResult {
		GraphSliceTask task;
		Graph graph;
		
		protected GraphSliceTaskResult(GraphSliceTask task, Graph graph) {
			this.task = task;
			this.graph = graph;
		}
	}
	
	protected class GraphSliceTask extends BaseAsyncTask implements Callable<Object> {

		protected String serviceId;
		protected String viewId;
		protected ViewInput input;
		protected VolumeType volumeType;
		protected DateTime from;
		protected DateTime to;
		protected int baselineWindow;
		protected int activeWindow;
		protected int pointsWanted;
		protected int windowSlice;
		protected GraphRequest.Builder builder;
		
		protected GraphSliceTask(GraphRequest.Builder builder, String serviceId, String viewId, int pointsWanted,
				ViewInput input, VolumeType volumeType, DateTime from, DateTime to,
				int baselineWindow, int activeWindow, int windowSlice) {
			
			this.builder = builder;
			this.serviceId = serviceId;
			this.viewId = viewId;
			this.pointsWanted = pointsWanted;
			this.input = input;
			this.volumeType = volumeType;
			this.from = from;
			this.to = to;
			this.baselineWindow = baselineWindow;
			this.activeWindow = activeWindow;
			this.windowSlice = windowSlice;
		}
		
		@Override
		public Object call() throws Exception
		{			
			Response<GraphResult> response = ApiCache.getEventGraph(apiClient, serviceId, input, volumeType,
					builder.build(), pointsWanted, baselineWindow, activeWindow, windowSlice);
			
			if (response.isBadResponse())
			{
				return null;
			}
			
			GraphResult graphResult = response.data;
			
			if (graphResult == null)
			{
				return null;
			}
			
			if (CollectionUtil.safeIsEmpty(graphResult.graphs))
			{
				return null;
			}
			
			Graph graph = graphResult.graphs.get(0);
			
			if (graph == null)
			{
				return null;
			}
			
			if (!viewId.equals(graph.id))
			{
				return null;
			}
			
			if (CollectionUtil.safeIsEmpty(graph.points))
			{
				return null;
			}
			
			return new GraphSliceTaskResult(this, graph)	;
		}
	}
	
	protected class SliceRequest {
		protected DateTime from;
		protected DateTime to;
		int pointCount;
		
		protected SliceRequest(DateTime from, DateTime to, int pointCount) {
			this.from = from;
			this.to = to;
			this.pointCount = pointCount;
		}
	}
	
	public GrafanaFunction(ApiClient apiClient)
	{
		this.apiClient = apiClient;
	}
	
	public static String toQualified(String value)
	{
		return value.replace(INTERNAL_DELIM, QUALIFIED_DELIM);
	}
	
	protected String toTransactionName(Location location)
	{
		String transactionName =
				location.class_name + TRANS_DELIM + location.method_name + TRANS_DELIM + location.method_desc;
		return transactionName;
	}
	
	protected Object getTimeValue(long value, FunctionInput input) {
		
		switch (input.getTimeFormat()) {
			
			case EPOCH: return Long.valueOf(value);
			case ISO_UTC: return TimeUtil.getDateTimeFromEpoch(value);
		}
		
		throw new IllegalStateException();
	}
	
	protected static Object getTimeValue(Long value, FunctionInput input) {
		
		switch (input.getTimeFormat()) {
			
			case EPOCH: return value;
			case ISO_UTC: return TimeUtil.getDateTimeFromEpoch(value.longValue());
		}
		
		throw new IllegalStateException();
	}
	
	protected Pair<Object, Object> getTimeFilterPair(Pair<DateTime, DateTime> timeSpan, String timeFilter) {
		
		Object from;
		Object to;
		
		String timeUnit = TimeUtil.getTimeUnit(timeFilter);
		
		if (timeUnit != null) {
			from = "now-" + timeUnit;
			to = "now";
		} else {
			from = timeSpan.getFirst().getMillis();
			to = timeSpan.getSecond().getMillis();
		}
		
		return Pair.of(from, to);
	}

	protected static Pair<String, String> getTransactionNameAndMethod(String name, boolean fullyQualified)
	{
		
		String[] parts = name.split(TRANS_DELIM);
		
		if (parts.length == 1)
		{
			if (fullyQualified) {
				return Pair.of(toQualified(name), null);
			} else {
				return Pair.of(getSimpleClassName(toQualified(name)), null);
			}
			
		}
		else
		{
			if (fullyQualified) {
				return Pair.of(toQualified((parts[0])), parts[1]);		
			} else {
				return Pair.of(getSimpleClassName(toQualified((parts[0]))), parts[1]);
			}
		}
	}
	
	protected static Pair<String, String> getFullNameAndMethod(String name)
	{
		
		String[] parts = name.split(TRANS_DELIM);
		
		if (parts.length == 1)
		{
			return Pair.of(toQualified(name), null);
		}
		else
		{
			return Pair.of(toQualified((parts[0])), parts[1]);
		}
	}
	
	protected static String getTransactionName(String name, boolean includeMethod)
	{
		
		Pair<String, String> nameAndMethod = getTransactionNameAndMethod(name, false);
		
		if ((includeMethod) && (nameAndMethod.getSecond() != null))
		{
			return nameAndMethod.getFirst() + QUALIFIED_DELIM + nameAndMethod.getSecond();
		}
		else
		{
			return nameAndMethod.getFirst();
		}
	}
	
	public static String getSimpleClassName(String className)
	{
		String qualified = toQualified(className);
		int sepIdex = Math.max(qualified.lastIndexOf(QUALIFIED_DELIM) + 1, 0);
		String result = qualified.substring(sepIdex, qualified.length());
		return result;
	}
	
	public static String getTypeAndSimpleClassName(String type, String className, String methodName)
	{
		String result="";
		
		if (type != null && !type.isEmpty()) {
				result += TYPES_MAP.get(type) + QUALIFIED_DELIM;
		}
		result += getSimpleClassName(className) + QUALIFIED_DELIM + methodName;
		return result;
	}
	
	public static String getTypeAbbr(String type)
	{
		String result="";
		
		if (type != null && !type.trim().isEmpty()) {
			String typeAbbrv = TYPES_MAP.get(type.trim());
			if (typeAbbrv != null && !typeAbbrv.isEmpty()) {
				result = typeAbbrv;
			} else {
				result = type.trim();
			}
		}
		return result;
	}

	protected void validateResponse(Response<?> response)
	{
		if ((response.isBadResponse()) || (response.data == null))
		{
			System.err.println("EventsResult code " + response.responseCode);
		}
		
	}
	
	protected static String formatLocation(Location location)
	{
		if (location == null) {
			return null;
		}
		
		return getSimpleClassName(location.class_name) + QUALIFIED_DELIM + location.method_name;
	}
	
	protected Collection<String> getServiceIds(BaseEnvironmentsInput input)
	{
		
		List<String> serviceIds = input.getServiceIds();
		
		if (serviceIds.size() == 0)
		{
			return Collections.emptyList();
		}
		
		List<String> result;
		
		if (input.unlimited) {
			result = serviceIds;
		} else {
			result = serviceIds.subList(0, Math.min(BaseEnvironmentsInput.MAX_COMBINE_SERVICES, 
				serviceIds.size()));
		}
		
		result.remove(NONE);
		
		return result;
	}
	
	private void applyBuilder(ViewTimeframeRequest.Builder builder, String serviceId, String viewId,
			Pair<String, String> timeSpan, ViewInput request)
	{
		
		builder.setViewId(viewId).setServiceId(serviceId).setFrom(timeSpan.getFirst()).setTo(timeSpan.getSecond());
		
		applyFilters(request, serviceId, builder);
	}
	
	public static boolean filterTransaction(GroupFilter filter, String searchText,
		String className, String methodName) {
		
		String searchTextLower;
		
		if (searchText != null) {
			searchTextLower = searchText.toLowerCase();
		} else {
			searchTextLower = null;
		}
		
		String simpleClassName = getSimpleClassName(className);
		String simpleClassAndMethod;
		
		if (methodName != null) {
			simpleClassAndMethod = simpleClassName + QUALIFIED_DELIM + methodName;
			
			if ((searchText != null) && (!simpleClassAndMethod.toLowerCase().contains(searchTextLower))) {
				return true;
			}
		} else {
			simpleClassAndMethod = null;
			
			if ((searchText != null) && (!simpleClassName.toLowerCase().contains(searchTextLower))) {
				return true;
			}
		}
	
		if ((filter == null) || ((filter.values.size() == 0) && (filter.patterns.size() == 0))) {		
			return false;
		}
				
		for (String value : filter.values)
		{
			if ((simpleClassAndMethod != null) && (value.equals(simpleClassAndMethod))) {				
				return false;
			}
			
			if (value.equals(simpleClassName)) {				
				return false;
			}
			
		}
		
		String fullName;
		
		if (methodName != null) {
			fullName = className + QUALIFIED_DELIM + methodName;
		} else {
			fullName = className;
		}
		
		for (Pattern pattern : filter.patterns)
		{
			if (pattern.matcher(fullName).find())
			{
				return false;
			}
		}
		
		return true;
	}
	
	protected Collection<Transaction> getTransactions(String serviceId, String viewId,
			Pair<DateTime, DateTime> timeSpan,
			ViewInput input, String searchText) {
		return getTransactions(serviceId, viewId, timeSpan, input, searchText, 0, 0);	
	}
	
	protected Collection<TransactionGraph> getTransactionGraphs(BaseEventVolumeInput input, String serviceId, String viewId, 
			Pair<DateTime, DateTime> timeSpan, String searchText,
			int pointsWanted, int activeTimespan, int baselineTimespan) {
		
		Pair<String, String> fromTo = TimeUtil.toTimespan(timeSpan);
		
		TransactionsGraphRequest.Builder builder = TransactionsGraphRequest.newBuilder().setServiceId(serviceId)
				.setViewId(viewId).setFrom(fromTo.getFirst()).setTo(fromTo.getSecond())
				.setWantedPointCount(pointsWanted);
				
		applyFilters(input, serviceId, builder);

		Response<TransactionsGraphResult> response = ApiCache.getTransactionsGraph(apiClient, serviceId, input,
			pointsWanted, baselineTimespan, activeTimespan, builder.build());
				
		validateResponse(response);
		
		if ((response.data == null) || (response.data.graphs == null)) { 

			return Collections.emptyList();
		}
		
		Collection<TransactionGraph> result;
		
		if ((input.hasTransactions() || (searchText != null)))
		{
			
			result = new ArrayList<TransactionGraph>(response.data.graphs.size());
			Collection<String> transactions = input.getTransactions(serviceId);
			
			GroupFilter transactionsFilter = GrafanaSettings.getServiceSettings(apiClient, serviceId).getTransactionsFilter(transactions);

			for (TransactionGraph transaction : response.data.graphs)
			{
				Pair<String, String> nameAndMethod = getFullNameAndMethod(transaction.name);
				
				if (filterTransaction(transactionsFilter, searchText, nameAndMethod.getFirst(), nameAndMethod.getSecond()))
				{
					continue;
				}
				
				result.add(transaction);
			}
			
		}
		else
		{
			result = response.data.graphs;
		}
		
		return result;
	}
	
	protected Collection<Transaction> getTransactions(String serviceId, String viewId,
			Pair<DateTime, DateTime> timeSpan,
			ViewInput input, String searchText, int activeTimespan, int baselineTimespan)
	{
		
		Pair<String, String> fromTo = TimeUtil.toTimespan(timeSpan);
		
		TransactionsVolumeRequest.Builder builder = TransactionsVolumeRequest.newBuilder().setServiceId(serviceId)
				.setViewId(viewId).setFrom(fromTo.getFirst()).setTo(fromTo.getSecond()).setRaw(true);
		
		applyFilters(input, serviceId, builder);
		
		Response<TransactionsVolumeResult> response = ApiCache.getTransactionsVolume(apiClient, serviceId,
				input, activeTimespan, baselineTimespan, builder.build());
		
		if (response.isBadResponse())
		{
			System.err.println("Transnaction volume for service " + serviceId + " code: " + response.responseCode);
		}
		
		if ((response.data == null) || (response.data.transactions == null))
		{
			return null;
		}
		
		Collection<Transaction> result;
		
		if ((input.hasTransactions() || (searchText != null)))
		{
			
			result = new ArrayList<Transaction>(response.data.transactions.size());
			Collection<String> transactions = input.getTransactions(serviceId);
			
			GroupFilter transactionsFilter = GrafanaSettings.getServiceSettings(apiClient, serviceId).getTransactionsFilter(transactions);

			for (Transaction transaction : response.data.transactions)
			{
				Pair<String, String> nameAndMethod = getFullNameAndMethod(transaction.name);
				
				if (filterTransaction(transactionsFilter, searchText, nameAndMethod.getFirst(), nameAndMethod.getSecond()))
				{
					continue;
				}
				
				result.add(transaction);
			}
			
		}
		else
		{
			result = response.data.transactions;
		}
		
		return result;
	}
	
	/**
	 * @param seriesName
	 *            - needed by child classes
	 * @param volumeType
	 */
	protected String getSeriesName(BaseGraphInput input, String seriesName, Object volumeType, String serviceId,
			Collection<String> serviceIds)
	{
		
		return getServiceValue(input.deployments, serviceId, serviceIds);
	}
	
	protected Graph getEventsGraph(String serviceId, String viewId, int pointsCount,
			ViewInput input, VolumeType volumeType, DateTime from, DateTime to) {
		return getEventsGraph(serviceId, viewId, pointsCount, input, volumeType, from, to, 0, 0);
	}
	
	protected Graph getEventsGraph(String serviceId, String viewId, int pointsCount,
			ViewInput input, VolumeType volumeType, DateTime from, DateTime to, int baselineWindow, int activeWindow) {
		return getEventsGraph(serviceId, viewId, pointsCount, input, volumeType, from, to, baselineWindow, activeWindow, true);
	}
	
	protected GraphSliceTask createGraphAsyncTask(String serviceId, String viewId, int pointsCount,
			ViewInput input, VolumeType volumeType, DateTime from, DateTime to, int baselineWindow, int activeWindow, int windowSlice) {
		
		GraphRequest.Builder builder = GraphRequest.newBuilder().setServiceId(serviceId).setViewId(viewId)
				.setGraphType(GraphType.view).setFrom(from.toString(dateTimeFormatter)).setTo(to.toString(dateTimeFormatter))
				.setVolumeType(volumeType).setWantedPointCount(pointsCount).setRaw(true);
		
		applyFilters(input, serviceId, builder);
		
		GraphSliceTask task = new GraphSliceTask(builder, serviceId, viewId, pointsCount, 
			input, volumeType, from, to, baselineWindow, activeWindow, windowSlice);
		
		return task;
	}
	
	protected Graph getEventsGraph(String serviceId, String viewId, int pointsCount,
			ViewInput input, VolumeType volumeType, DateTime from, DateTime to, int baselineWindow, int activeWindow, boolean sync) {		
		
		Collection<GraphSliceTask> tasks = getGraphTasks(serviceId, viewId, 
				pointsCount, input, volumeType, from, to, baselineWindow, activeWindow, sync);

		Collection<GraphSliceTaskResult> graphTasks = executeGraphTasks(tasks, sync);
			
		Graph result = mergeGraphs(graphTasks);
		
		return result;
	}
	
	protected Collection<GraphSliceTask> getGraphTasks(String serviceId, String viewId, int pointsCount,
			ViewInput input, VolumeType volumeType, DateTime from, DateTime to, 
			int baselineWindow, int activeWindow, boolean sync)
	{
		int days = Math.abs(Days.daysBetween(from, to).getDays());
		
		int effectivePoints;
		List<SliceRequest> sliceRequests;
		
		if ((sync) || ((days < 3) || (days > 14))) // This is just a starting point
		{
			effectivePoints = pointsCount;
			sliceRequests = Collections.singletonList(new SliceRequest(from, to, pointsCount));
		}
		else
		{
			effectivePoints = (pointsCount / days) + 1;
			sliceRequests = getTimeSlices(from, to, days, effectivePoints);	
		}
		
		List<GraphSliceTask> tasks = Lists.newArrayList();
		
		int index = 0;
		
		for (SliceRequest sliceRequest : sliceRequests)
		{
			int sliceIndex;
			
			if (sync) {
				sliceIndex = NO_GRAPH_SLICE;
			} else {
				sliceIndex = index;
			}
				
			GraphSliceTask task = createGraphAsyncTask(serviceId, viewId, sliceRequest.pointCount, input, volumeType, 
					sliceRequest.from, sliceRequest.to, baselineWindow, activeWindow, sliceIndex);
				
			index++;
			
			tasks.add(task);
		}
		
		return tasks;
	}
	
	private List<SliceRequest> getTimeSlices(DateTime from, DateTime to, int days, int pointCount) {
		
		List<SliceRequest> result = Lists.newArrayList();
		
		// First partial day (<2018-11-22T12:23:38.418+02:00, 2018-11-22T23:59:00.000+02:00>)
		
		result.add(new SliceRequest(from, from.plusDays(1).withTimeAtStartOfDay().minusMinutes(1), END_SLICE_POINT_COUNT));

		// Only full days (<2018-11-23T00:00:00.000+02:00, 2018-11-23T23:59:00.000+02:00>)
		for (int i = 1; i < days; i++)
		{
			DateTime fullDayStart = from.plusDays(i).withTimeAtStartOfDay(); 
			DateTime fullDayEnd = fullDayStart.plusDays(1).withTimeAtStartOfDay().minusMinutes(1);
			
			result.add(new SliceRequest(fullDayStart, fullDayEnd, pointCount));
		}
		
		// Last partial day (<2018-11-29T00:00:00.000+02:00, 2018-11-29T12:23:38.418+02:00>)
		
		result.add(new SliceRequest(to.withTimeAtStartOfDay(), to, END_SLICE_POINT_COUNT));
		
		return result;
	}
	
	protected Graph mergeGraphs(Collection<GraphSliceTaskResult> graphTasks ) {
		
		if (graphTasks.size() == 0) {
			return null;
		}
		
		if (graphTasks.size() == 1) {
			return graphTasks.iterator().next().graph;
		}
		
		Graph result = new Graph();		
		Map<Long, GraphPoint> graphPoints = new TreeMap<Long, GraphPoint>();
		
		for (Object taskResult : graphTasks) {
			
			if (taskResult == null) {
				continue;
			}
			
			GraphSliceTaskResult graphSliceTaskResult = (GraphSliceTaskResult)taskResult;
			
			if (result.id == null) {
				result.id = graphSliceTaskResult.graph.id;
				result.type = graphSliceTaskResult.graph.type;
			}
			
			for (GraphPoint gp : graphSliceTaskResult.graph.points) {
				long epoch = TimeUtil.getLongTime(gp.time);
				graphPoints.put(Long.valueOf(epoch), gp);
			}			
		}
		
		result.points = new ArrayList<GraphPoint>(graphPoints.values());
		
		return result;
	}
	
	protected Collection<GraphSliceTaskResult> executeGraphTasks(Collection<GraphSliceTask> slices, boolean sync) {
		
		List<Callable<Object>> tasks = Lists.newArrayList(slices);
		
		Collection<Object> taskResults;
		
		if (sync) {
			taskResults = Lists.newArrayList();
			
			for (Callable<Object> task : tasks) {
				try
				{
					taskResults.add(task.call());
				}
				catch (Exception e)
				{
					throw new IllegalStateException(e);
				}
			}
		} else {	
			taskResults = executeTasks(tasks, true);	
		}
		
		List<GraphSliceTaskResult> result = Lists.newArrayList();
		
		for (Object taskResult : taskResults) {
			
			if (taskResult == null) {
				continue;
			}
			
			result.add((GraphSliceTaskResult)taskResult);
		}
			
		return result;
	}
	
	private static void appendGraphStats(Map<String, EventResult> eventMap, Graph graph)
	{
		
		for (GraphPoint gp : graph.points)
		{
			
			if (gp.contributors == null)
			{
				continue;
			}
			
			for (GraphPointContributor gpc : gp.contributors)
			{
				
				EventResult event = eventMap.get(gpc.id);
				
				if (event == null)
				{
					continue;
				}
				
				if (event.stats == null)
				{
					event.stats = new Stats();
				}
				
				event.stats.hits += gpc.stats.hits;
				event.stats.invocations += gpc.stats.invocations;
			}
		}
	}
	
	private Collection<EventResult> getEventListFromGraph(String serviceId, String viewId, ViewInput input,
			DateTime from, DateTime to,
			VolumeType volumeType, int pointsCount)
	{
		
		Graph graph = getEventsGraph(serviceId, viewId, pointsCount, input, volumeType, from, to);
		
		if (graph == null)
		{
			return null;
		}
		
		Collection<EventResult> events = getEventList(serviceId, viewId, input, from, to);
		
		if (events == null)
		{
			return null;
		}
		
		Map<String, EventResult> eventsMap = getEventsMap(events);
		appendGraphStats(eventsMap, graph);
		
		return eventsMap.values();
	}
	
	protected EventsSlimVolumeResult getEventsVolume(String serviceId, String viewId, ViewInput input, DateTime from,
			DateTime to,
			VolumeType volumeType)
	{	
		EventsSlimVolumeRequest.Builder builder =
				EventsSlimVolumeRequest.newBuilder().setVolumeType(volumeType).setServiceId(serviceId).setViewId(viewId)
						.setFrom(from.toString(dateTimeFormatter)).setTo(to.toString(dateTimeFormatter))
						.setVolumeType(volumeType).setRaw(true);
		
		applyBuilder(builder, serviceId, viewId, TimeUtil.toTimespan(from, to), input);
		
		Response<EventsSlimVolumeResult> response =
				ApiCache.getEventVolume(apiClient, serviceId, input, volumeType, builder.build());
		
		validateResponse(response);
		
		if ((response.data == null) || (response.data.events == null))
		{
			return null;
		}
		
		return response.data;
	}
	
	private void applyVolumeToEvents(String serviceId, String viewId, ViewInput input, DateTime from, DateTime to,
			VolumeType volumeType, Map<String, EventResult> eventsMap)
	{
		EventsSlimVolumeResult eventsSlimVolumeResult = getEventsVolume(serviceId, viewId, input, from, to, volumeType);
		
		if (eventsSlimVolumeResult == null) {
			return;
		}
		
		for (EventSlimResult eventSlimResult : eventsSlimVolumeResult.events)
		{
			EventResult event = eventsMap.get(eventSlimResult.id);
			
			if (event == null)
			{
				continue;
			}
			
			event.stats = eventSlimResult.stats;
		}
	}
	
	private Collection<EventResult> getEventList(String serviceId, String viewId, ViewInput input, DateTime from,
			DateTime to)
	{
		EventsRequest.Builder builder = EventsRequest.newBuilder().setRaw(true);
		applyBuilder(builder, serviceId, viewId, TimeUtil.toTimespan(from, to), input);
		
		Response<?> response = ApiCache.getEventList(apiClient, serviceId, input, builder.build());
		validateResponse(response);
		
		List<EventResult> events;
		
		if (response.data instanceof EventsVolumeResult)
		{
			events = ((EventsVolumeResult)(response.data)).events;
		}
		else if (response.data instanceof EventsResult)
		{
			events = ((EventsResult)(response.data)).events;
		}
		else
		{
			return null;
		}
		
		if (events == null)
		{
			return null;
		}
		
		List<EventResult> eventsCopy = new ArrayList<EventResult>(events.size());
		
		try
		{
			for (EventResult event : events)
			{
				EventResult clone = (EventResult)event.clone();
				clone.stats = new Stats();
				eventsCopy.add(clone);
			}
		}
		catch (CloneNotSupportedException e)
		{
			throw new IllegalStateException(e);
			
		}
		
		return eventsCopy;
	}
	
	private static Map<String, EventResult> getEventsMap(Collection<EventResult> events)
	{
		
		Map<String, EventResult> result = new HashMap<String, EventResult>();
		
		for (EventResult event : events)
		{
			result.put(event.id, event);
		}
		
		return result;
	}
	
	protected Map<String, EventResult> getEventMap(String serviceId, ViewInput input, DateTime from, DateTime to,
			VolumeType volumeType)
	{
		return getEventMap(serviceId, input, from, to, volumeType, 0);
	}
	
	protected Map<String, EventResult> getEventMap(String serviceId, ViewInput input, DateTime from, DateTime to,
			VolumeType volumeType, int pointsCount)
	{
		
		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null)
		{
			return null;
		}
		
		Collection<EventResult> events;
		
		Map<String, EventResult> result = new HashMap<String, EventResult>();
		
		if (volumeType != null)
		{
			if (pointsCount > 0)
			{
				events = getEventListFromGraph(serviceId, viewId, input, from, to, volumeType, pointsCount);
			}
			else
			{
				events = getEventList(serviceId, viewId, input, from, to);
				
				if (events != null) {
					applyVolumeToEvents(serviceId, viewId, input, from, to, 
						volumeType, getEventsMap(events));
				}
			}
		}
		else
		{
			events = getEventList(serviceId, viewId, input, from, to);
		}
		
		if (events == null)
		{
			return null;
			
		}
		
		result = getEventsMap(events);
		
		return result;
	}
	
	protected List<Object> executeTasks(Collection<Callable<Object>> tasks, boolean queryPool)
	{	
		Executor executor;
		
		if (queryPool) {
			executor = GrafanaThreadPool.getQueryExecutor(apiClient);
		} else {
			executor  = GrafanaThreadPool.getFunctionExecutor(apiClient);
		} 
		
		CompletionService<Object> completionService = new ExecutorCompletionService<Object>(executor);
		
		for (Callable<Object> task : tasks)
		{
			completionService.submit(task);
		}
		
		List<Object> result = new ArrayList<Object>();
		
		int received = 0;
		
		while (received < tasks.size())
		{
			try
			{
				Future<Object> future = completionService.take();
				
				received++;
				Object asynResult = future.get();
				result.add(asynResult);
			}
			catch (Exception e)
			{
				throw new IllegalStateException(e);
			}
		}
		
		return result;
	}
	
	protected void applyFilters(EnvironmentsFilterInput input, String serviceId, TimeframeRequest.Builder builder)
	{
		applyFilters(this.apiClient, input, serviceId, builder);
	}
	
	private void applyFilters(ApiClient apiClient, EnvironmentsFilterInput input, String serviceId,
			TimeframeRequest.Builder builder)
	{
		
		for (String app : input.getApplications(apiClient, serviceId))
		{
			builder.addApp(app);
		}
		
		for (String dep : input.getDeployments(serviceId))
		{
			builder.addDeployment(dep);
		}
		
		for (String server : input.getServers(serviceId))
		{
			builder.addServer(server);
		}
	}
	
	protected SummarizedView getView(String serviceId, String viewName)
	{
		
		if (viewName.startsWith(GRAFANA_VAR_PREFIX))
		{
			return null;
		}
		
		ViewsRequest request = ViewsRequest.newBuilder().setServiceId(serviceId).setViewName(viewName).build();
		
		Response<ViewsResult> response = ApiCache.getView(apiClient, serviceId, viewName, request);
		
		if ((response.isBadResponse()) ||	(response.data == null) || (response.data.views == null) ||
			(response.data.views.size() == 0))
		{
			return null;
		}
		
		SummarizedView result = response.data.views.get(0);
		
		return result;
	}
	
	protected static String getServiceValue(String value, String serviceId)
	{
		return value + SERVICE_SEPERATOR + serviceId;
	}
	
	protected static String getServiceValue(String value, String serviceId, Collection<String> serviceIds)
	{
		
		if (serviceIds.size() == 1)
		{
			return value;
		}
		else
		{
			return getServiceValue(value, serviceId);
		}
	}
	
	protected String getViewId(String serviceId, String viewName)
	{
		SummarizedView view = getView(serviceId, viewName);
		
		if (view == null)
		{
			return null;
		}
		
		return view.id;
	}
	
	protected List<Series> createSingleStatSeries(Pair<DateTime, DateTime> timespan, Object singleStat)
	{
		
		Series series = new Series();
		
		series.name = SERIES_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, SUM_COLUMN });
		
		Long time = Long.valueOf(timespan.getSecond().getMillis());
		series.values = Collections.singletonList(Arrays.asList(new Object[] { time, singleStat }));
		
		return Collections.singletonList(series);
	}
	
	public abstract List<Series> process(FunctionInput functionInput);
}
