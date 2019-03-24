package com.takipi.integrations.grafana.functions;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.ocpsoft.prettytime.PrettyTime;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.event.Location;
import com.takipi.api.client.data.event.Stats;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.data.process.Jvm;
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
import com.takipi.api.client.result.process.JvmsResult;
import com.takipi.api.client.result.transaction.TransactionsGraphResult;
import com.takipi.api.client.result.transaction.TransactionsVolumeResult;
import com.takipi.api.client.result.view.ViewsResult;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.performance.PerformanceUtil;
import com.takipi.api.client.util.performance.calc.PerformanceCalculator;
import com.takipi.api.client.util.performance.calc.PerformanceScore;
import com.takipi.api.client.util.performance.calc.PerformanceState;
import com.takipi.api.client.util.performance.transaction.GraphPerformanceCalculator;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.api.client.util.regression.settings.GeneralSettings;
import com.takipi.api.client.util.regression.settings.GroupSettings;
import com.takipi.api.client.util.regression.settings.GroupSettings.GroupFilter;
import com.takipi.api.client.util.regression.settings.ServiceSettingsData;
import com.takipi.api.client.util.regression.settings.SlowdownSettings;
import com.takipi.api.client.util.transaction.TransactionUtil;
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
import com.takipi.integrations.grafana.input.VariableInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.TimeUtil;

public abstract class GrafanaFunction {	
	
	public interface FunctionFactory {
		
		public GrafanaFunction create(ApiClient apiClient);
		public Class<?> getInputClass();
		public String getName();
	}
	
	private static final DecimalFormat singleDigitFormatter = new DecimalFormat("#.#");
	private static final DecimalFormat doubleDigitFormatter = new DecimalFormat("#.##");

	protected static final PrettyTime prettyTime = new PrettyTime();
	
	protected static final String ALL_EVENTS = "All Events";
	
	protected static final String RESOLVED = "Resolved";
	protected static final String HIDDEN = "Hidden";
	
	protected static final String SERIES_NAME = "events";
	protected static final String EMPTY_NAME = "";
	
	protected static final String SUM_COLUMN = "sum";
	protected static final String TIME_COLUMN = "time";
	protected static final String KEY_COLUMN = "key";
	protected static final String VALUE_COLUMN = "value";
	
	public static final String GRAFANA_SEPERATOR_RAW = "|";
	public static final String ARRAY_SEPERATOR_RAW = ServiceSettingsData.ARRAY_SEPERATOR_RAW;
	public static final String GRAFANA_VAR_ADD = "And";

	public static final String GRAFANA_SEPERATOR = Pattern.quote(GRAFANA_SEPERATOR_RAW);
	public static final String ARRAY_SEPERATOR = Pattern.quote(ARRAY_SEPERATOR_RAW);
	
	public static final String SERVICE_SEPERATOR_RAW = ":";
	public static final String SERVICE_SEPERATOR = SERVICE_SEPERATOR_RAW + " ";
	public static final String GRAFANA_VAR_PREFIX = "$";
	
	public static final String ALL = "All";
	public static final String NONE = "None";
	public static final List<String> VAR_ALL = Arrays.asList(new String[] { "*", ALL, 
		ALL.toLowerCase(), NONE, NONE.toLowerCase() });
	
	protected static final char QUALIFIED_DELIM = '.';
	protected static final char INTERNAL_DELIM = '/';
	protected static final String TRANS_DELIM = "#";
	protected static final String EMPTY_POSTFIX = ".";
	
	protected static final String QUALIFIED_DELIM_PATTERN = Pattern.quote(String.valueOf(GrafanaFunction.QUALIFIED_DELIM));
	
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

	public static final int TOP_TRANSACTIONS_MAX = 3;
	public static final String TOP_ERRORING_TRANSACTIONS = String.format("Top %d Failing", TOP_TRANSACTIONS_MAX);
	public static final String SLOWEST_TRANSACTIONS = String.format("Top %d Slowest", TOP_TRANSACTIONS_MAX);
	public static final String SLOWING_TRANSACTIONS = String.format("Top %d Slowing", TOP_TRANSACTIONS_MAX);
	public static final String HIGHEST_VOLUME_TRANSACTIONS = String.format("Top %d Volume", TOP_TRANSACTIONS_MAX);
		
	public static final List<String> TOP_TRANSACTION_FILTERS = Arrays.asList(new String[] {
			TOP_ERRORING_TRANSACTIONS, 	SLOWEST_TRANSACTIONS, SLOWING_TRANSACTIONS, HIGHEST_VOLUME_TRANSACTIONS});
	
	static {
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
	protected Map<String, ServiceSettingsData> settingsMaps;
	
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
		public Object call() throws Exception {		
			Response<GraphResult> response = ApiCache.getEventGraph(apiClient, serviceId, input, volumeType,
				builder.build(), pointsWanted, baselineWindow, activeWindow, windowSlice);
			
			if (response.isBadResponse()) {
				return null;
			}
			
			GraphResult graphResult = response.data;
			
			if (graphResult == null) {
				return null;
			}
			
			if (CollectionUtil.safeIsEmpty(graphResult.graphs)) {
				return null;
			}
			
			Graph graph = graphResult.graphs.get(0);
			
			if (graph == null) {
				return null;
			}
			
			if (!viewId.equals(graph.id)) {
				return null;
			}
			
			if (CollectionUtil.safeIsEmpty(graph.points)) {
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
	
	protected class TransactionData {
		protected com.takipi.api.client.data.transaction.Stats stats;
		protected TransactionGraph graph;
		protected TransactionGraph baselineGraph;
		protected long timersHits;
		protected long errorsHits;
		protected List<EventResult> errors;
		protected EventResult currTimer;
		protected PerformanceState state;
		protected double score;
		protected double baselineAvg;
		protected long baselineInvocations;
	}
	
	public static class TransactionKey {
		
		public String className;
		public String methodName;
		
		public static TransactionKey of(String className, String methodName) {
			
			TransactionKey result = new TransactionKey();
			result.className = className;
			result.methodName = methodName;
			
			return result;
		}
		
		
		@Override
		public boolean equals(Object obj) {
			
			if (!(obj instanceof TransactionKey)) {
				return false;
			}
			
			TransactionKey other = (TransactionKey)obj;
			
			if (!Objects.equal(className, other.className)) {
				return false;
			}
			
			if (!Objects.equal(methodName, other.methodName)) {
				return false;
			}
			
			return true;
		}
		
		@Override
		public int hashCode() {
			
			if (methodName == null) {
				return className.hashCode();
			}
			
			return className.hashCode() ^ methodName.hashCode();
		}
	}
	
	public static class TransactionDataResult {
		
		public Map<TransactionKey, TransactionData> items;
		public RegressionInput regressionInput;
		public RegressionWindow regressionWindow;
	}
	
	protected abstract class TopTransactionProcessor {
		
		protected abstract Comparator<Map.Entry<TransactionKey, TransactionData>> getComparator(TransactionDataResult transactionDataResult);
		
		/**
		 * @param key - needed for children
		 * @param data 
		 */
		protected boolean includeTransaction(TransactionKey key, TransactionData data) {
			return true;
		}
		
		protected Collection<String> getTransactions(TransactionDataResult transactionDataResult) {
			
			if ((transactionDataResult == null) || (transactionDataResult.items.size() == 0)) {
				return Collections.emptyList();
			}
			
			Comparator<Map.Entry<TransactionKey, TransactionData>> comparator = getComparator(transactionDataResult);
			
			List<Map.Entry<TransactionKey, TransactionData>> sortedTransactionDatas = 
					new ArrayList<Map.Entry<TransactionKey, TransactionData>>(transactionDataResult.items.entrySet());
				
			sortedTransactionDatas.sort(comparator);
			int size = Math.min(sortedTransactionDatas.size(), TOP_TRANSACTIONS_MAX);
			
			List<String> result = new ArrayList<String>(size);
			
			for (Map.Entry<TransactionKey, TransactionData> entry : sortedTransactionDatas) {
				
				if (!includeTransaction(entry.getKey(), entry.getValue())) {
					continue;
				}
				
				result.add(getSimpleClassName(entry.getKey().className));
				
				if (result.size() >= size) {
					break;
				}
			}
	
			return result;
		}
	}
	
	protected class TopErrorTransactionProcessor extends TopTransactionProcessor {
	
		@Override
		protected Comparator<Map.Entry<TransactionKey, TransactionData>> getComparator(TransactionDataResult transactionDataResult) {
			
			return new Comparator<Map.Entry<TransactionKey,TransactionData>>() {
	
				@Override
				public int compare(Entry<TransactionKey, TransactionData> o1, Entry<TransactionKey, TransactionData> o2) {
					long v1 =  o1.getValue().errorsHits;
					long v2 =  o2.getValue().errorsHits;
					
					if (v2 - v1 > 0) {
						return 1;
					}
					
					if (v2 - v1 < 0) {
						return -1;
					}
					
					return 0;
				}
			};
		}
	}
	
	protected class TopVolumeTransactionProcessor extends TopTransactionProcessor {
		
		@Override
		protected Comparator<Map.Entry<TransactionKey, TransactionData>> getComparator(TransactionDataResult transactionDataResult) {
				
			return new Comparator<Map.Entry<TransactionKey,TransactionData>>() {
	
				@Override
				public int compare(Entry<TransactionKey, TransactionData> o1, Entry<TransactionKey, TransactionData> o2) {
					long v1 =  o1.getValue().stats.invocations;
					long v2 =  o2.getValue().stats.invocations;
					
					if (v2 - v1 > 0) {
						return 1;
					}
					
					if (v2 - v1 < 0) {
						return -1;
					}
					
					return 0;
				}
			};
		}
	}
	
	protected class TopSlowestTransactionProcessor extends TopTransactionProcessor {
		
		@Override
		protected Comparator<Map.Entry<TransactionKey, TransactionData>> getComparator(TransactionDataResult transactionDataResult) {
			
			return new Comparator<Map.Entry<TransactionKey,TransactionData>>() {
	
				@Override
				public int compare(Entry<TransactionKey, TransactionData> o1, Entry<TransactionKey, TransactionData> o2){
					
					double v1 =  o1.getValue().stats.avg_time;
					double v2 =  o2.getValue().stats.avg_time;
					
					if (v2 - v1 > 0) {
						return 1;
					}
					
					if (v2 - v1 < 0) {
						return -1;
					}
					
					return 0;
				}
			};
		}
	}
	
	protected class TopSlowingTransactionProcessor extends TopTransactionProcessor {
		
		@Override
		protected Collection<String> getTransactions(TransactionDataResult transactionDataResult) {
			if ((transactionDataResult == null) || (transactionDataResult.items.size() == 0)) {
				return Collections.emptyList();
			}
			
			boolean hasSlowdown = false;
			
			for (TransactionData transactionData : transactionDataResult.items.values()) {
				
				if ((transactionData.state == PerformanceState.SLOWING) 
				|| (transactionData.state == PerformanceState.CRITICAL)) {
					hasSlowdown = true;
					break;
				}
			}
			
			if (!hasSlowdown) {
				return null; 
			}
			
			return super.getTransactions(transactionDataResult);
		}
		
		
		@Override
		protected boolean includeTransaction(TransactionKey key, TransactionData data) {
			
			if ((data.state == PerformanceState.CRITICAL) 
			|| (data.state == PerformanceState.SLOWING)) {
				return true;
			}
			
			return false;
		}
		
		@Override
		protected Comparator<Map.Entry<TransactionKey, TransactionData>> getComparator(TransactionDataResult transactionDataResult) {
			
			return new Comparator<Map.Entry<TransactionKey,TransactionData>>() {
	
				@Override
				public int compare(Entry<TransactionKey, TransactionData> o1, Entry<TransactionKey, TransactionData> o2)
				{
					int v1 =  o1.getValue().state.ordinal();
					int v2 =  o2.getValue().state.ordinal();
					
					if (v2 - v1 > 0) {
						return 1;
					}
					
					if (v2 - v1 < 0) {
						return -1;
					}
					
					double d1 =  o1.getValue().score;
					double d2 =  o2.getValue().score;
					
					if (d2 - d1 > 0) {
						return 1;
					}
					
					if (d2 - d1 < 0) {
						return -1;
					}
					return 0;
				}
			};
		}
	}
	
	public GrafanaFunction(ApiClient apiClient, Map<String, ServiceSettingsData> settingsMaps) {
		this.apiClient = apiClient;
		this.settingsMaps = settingsMaps;
	}
	
	public GrafanaFunction(ApiClient apiClient) {
		this(apiClient, null);
	}
			
	public ServiceSettingsData getSettings(String serviceId) {
		
		ServiceSettingsData result = null;
		
		if (settingsMaps != null) {
			
			result = settingsMaps.get(serviceId);
			
			if (result != null) {
				return result;
			}
		} else {
			synchronized (this) {
				
				if (settingsMaps == null) {
					settingsMaps = Collections.synchronizedMap(new TreeMap<String, ServiceSettingsData>());
					result = doGetSettings(serviceId);
				}		
			}		
		}
		
		if (result == null) {
			result = doGetSettings(serviceId);
		}
		
		return result;
	}
	
	private ServiceSettingsData doGetSettings(String serviceId) {
		ServiceSettingsData result = GrafanaSettings.getServiceSettings(apiClient, serviceId).getData();
		settingsMaps.put(serviceId, result);
		
		return result;
	}
	
	public static String toQualified(String value) {
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

	protected static Pair<String, String> getTransactionNameAndMethod(String name, 
		boolean fullyQualified) {
		
		String[] parts = name.split(TRANS_DELIM);
		
		if (parts.length == 1) {
			if (fullyQualified) {
				return Pair.of(toQualified(name), null);
			} else {
				return Pair.of(getSimpleClassName(toQualified(name)), null);
			}
			
		} else {
			if (fullyQualified) {
				return Pair.of(toQualified((parts[0])), parts[1]);		
			} else {
				return Pair.of(getSimpleClassName(toQualified((parts[0]))), parts[1]);
			}
		}
	}
	
	protected static Pair<String, String> getFullNameAndMethod(String name) {
		
		String[] parts = name.split(TRANS_DELIM);
		
		if (parts.length == 1) {
			return Pair.of(toQualified(name), null);
		} else {
			return Pair.of(toQualified((parts[0])), parts[1]);
		}
	}
	
	protected static String getTransactionName(String name, boolean includeMethod) {
		
		Pair<String, String> nameAndMethod = getTransactionNameAndMethod(name, false);
		
		if ((includeMethod) && (nameAndMethod.getSecond() != null)){
			return nameAndMethod.getFirst() + QUALIFIED_DELIM + nameAndMethod.getSecond();
		} else {
			return nameAndMethod.getFirst();
		}
	}
	
	public static String getSimpleClassName(String className) {
		String qualified = toQualified(className);
		int sepIdex = Math.max(qualified.lastIndexOf(QUALIFIED_DELIM) + 1, 0);
		String result = qualified.substring(sepIdex, qualified.length());
		return result;
	}
	
	protected void validateResponse(Response<?> response) {
		if ((response.isBadResponse()) || (response.data == null)) {
			System.err.println("EventsResult code " + response.responseCode);
		}	
	}
	
	protected static String formatLocation(String className, String method) {
		return getSimpleClassName(className) + QUALIFIED_DELIM + method;
	}
	
	protected static String formatLocation(Location location) {
		
		if (location == null) {
			return null;
		}
		
		return formatLocation(getSimpleClassName(location.class_name), 
			location.method_name);
	}
	
	protected Collection<String> getServiceIds(BaseEnvironmentsInput input) {
		
		List<String> serviceIds = input.getServiceIds();
		
		if (serviceIds.size() == 0) {
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
			Pair<String, String> timeSpan, ViewInput request) {
		
		builder.setViewId(viewId).setServiceId(serviceId).
			setFrom(timeSpan.getFirst()).setTo(timeSpan.getSecond());
		
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
		
		boolean hasSearchText = (searchText != null) && (!searchText.equals(EventFilter.TERM));
		
		String simpleClassName = getSimpleClassName(className);
		String simpleClassAndMethod;
		
		if (methodName != null) {
			simpleClassAndMethod = simpleClassName + QUALIFIED_DELIM + methodName;
			
			if ((hasSearchText) && (!simpleClassAndMethod.toLowerCase().contains(searchTextLower))) {
				return true;
			}
		} else {
			simpleClassAndMethod = null;
			
			if ((hasSearchText) && (!simpleClassName.toLowerCase().contains(searchTextLower))) {
				return true;
			}
		}
	
		if ((filter == null) || ((filter.values.size() == 0) 
		&& (filter.patterns.size() == 0))) {		
			return false;
		}
				
		for (String value : filter.values) {
			if ((simpleClassAndMethod != null) 
			&& (value.equals(simpleClassAndMethod))) {				
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
		
		for (Pattern pattern : filter.patterns) {
			if (pattern.matcher(fullName).find()) {
				return false;
			}
		}
		
		return true;
	}
	
	protected Collection<TransactionGraph> getBaselineTransactionGraphs(
		String serviceId, String viewId, BaseEventVolumeInput input, 
		@SuppressWarnings("unused") Pair<DateTime, DateTime> timeSpan, RegressionInput regressionInput, 
		RegressionWindow regressionWindow) {
		
		BaseEventVolumeInput baselineInput;
		
		if ((input.hasDeployments()) || (input.hasTransactions())) {
			Gson gson = new Gson();
			String json = gson.toJson(input);
			baselineInput = gson.fromJson(json, input.getClass());
			baselineInput.deployments = null;
			baselineInput.transactions = null;
		} else {
			baselineInput = input;
		}
		
		DateTime baselineStart = regressionWindow.activeWindowStart.minusMinutes(regressionInput.baselineTimespan);

		Collection<TransactionGraph> result = getTransactionGraphs(baselineInput, serviceId, 
				viewId, Pair.of(baselineStart, regressionWindow.activeWindowStart), 
				null, baselineInput.pointsWanted, 
				regressionWindow.activeTimespan, regressionInput.baselineTimespan);
		
		return result;
	}
	
	private static com.takipi.api.client.data.transaction.Stats getTrasnactionGraphStats(TransactionGraph graph) {
		
		com.takipi.api.client.data.transaction.Stats result = new com.takipi.api.client.data.transaction.Stats();
		
		if (graph.points == null) {
			return result;
		}
				
		for (com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint gp : graph.points) {
			if (gp.stats != null) {
				result.invocations += gp.stats.invocations;
			}
		}
		
		double avgTimeSum = 0;
		
		for (com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint gp : graph.points) {
			if (gp.stats != null) {
				avgTimeSum += gp.stats.avg_time * gp.stats.invocations;
			}
		}
		
		result.avg_time = avgTimeSum / result.invocations;
		
		
		return result;
	}
	
	protected TransactionData getEventTransactionData(Map<TransactionKey, TransactionData> transactions, EventResult event) {
	
		TransactionKey classOnlyKey = TransactionKey.of(event.entry_point.class_name, null);
		TransactionData result = transactions.get(classOnlyKey);
	
		if (result == null) {
			
			TransactionKey classAndMethodKey = TransactionKey.of(event.entry_point.class_name, 
				event.entry_point.method_name);
			
			result = transactions.get(classAndMethodKey);
		}
		
		return result;
	}
	
	private Pair<RegressionInput, RegressionWindow> updateTransactionPerformance(String serviceId, String viewId, 
			Pair<DateTime, DateTime> timeSpan, BaseEventVolumeInput input, 
			Map<TransactionKey, TransactionData> transactionDatas) {
		
		RegressionFunction regressionFunction = new RegressionFunction(apiClient, settingsMaps);
		
		Pair<RegressionInput, RegressionWindow> result = regressionFunction.getRegressionInput(serviceId, 
				viewId, input, timeSpan, false);
		
		if (result == null) {
			return null;
		}
		
		RegressionInput regressionInput = result.getFirst();
		RegressionWindow regressionWindow = result.getSecond();
		
		Collection<TransactionGraph> baselineTransactionGraphs = getBaselineTransactionGraphs(serviceId, 
			viewId, input, timeSpan, regressionInput, regressionWindow);
	
		updateTransactionPerformance(serviceId, baselineTransactionGraphs, transactionDatas);
		
		return result;
		
	}
	
	protected void updateTransactionPerformance(String serviceId, 
			Collection<TransactionGraph> baselineTransactionGraphs, 
			Map<TransactionKey, TransactionData> transactionDatas) {

		SlowdownSettings slowdownSettings = getSettings(serviceId).slowdown;
		
		if (slowdownSettings == null) {
			throw new IllegalStateException("Missing slowdown settings for " + serviceId);
		}
		
		PerformanceCalculator<TransactionGraph> calc = GraphPerformanceCalculator.of(
				slowdownSettings.active_invocations_threshold, slowdownSettings.baseline_invocations_threshold,
				slowdownSettings.min_delta_threshold,
				slowdownSettings.over_avg_slowing_percentage, slowdownSettings.over_avg_critical_percentage,
				slowdownSettings.std_dev_factor);
				
		Map<String, TransactionGraph> activeGraphsMap = new HashMap<String, TransactionGraph>();
		
		for (TransactionData transactionData : transactionDatas.values()) {
			activeGraphsMap.put(transactionData.graph.name, transactionData.graph);
		}
		
		Map<String, TransactionGraph> baselineGraphsMap = TransactionUtil.getTransactionGraphsMap(baselineTransactionGraphs);
		
		Map<TransactionGraph, PerformanceScore> performanceScores = PerformanceUtil.getPerformanceStates(
				activeGraphsMap, baselineGraphsMap, calc);
		
		for (Map.Entry<TransactionGraph, PerformanceScore> entry : performanceScores.entrySet()) {
			
			String transactionName = entry.getKey().name;
			PerformanceScore performanceScore = entry.getValue();
			
			Pair<String, String> graphPair = getTransactionNameAndMethod(transactionName, true);
			TransactionKey key = TransactionKey.of(graphPair.getFirst(), graphPair.getSecond());
			TransactionData transactionData = transactionDatas.get(key);
			
			if (transactionData == null) {
				continue;
			}
			
			transactionData.state = performanceScore.state;
			transactionData.score = performanceScore.score;
			transactionData.stats = getTrasnactionGraphStats(transactionData.graph);

			transactionData.baselineGraph = baselineGraphsMap.get(transactionName);
		
			if (transactionData.baselineGraph != null) {
				com.takipi.api.client.data.transaction.Stats baselineStats = getTrasnactionGraphStats(transactionData.baselineGraph);
				
				if (baselineStats != null) {
					transactionData.baselineAvg = baselineStats.avg_time;
					transactionData.baselineInvocations = baselineStats.invocations;
				}
			}
		}		
	}
	
	private void updateTransactionEvents(String serviceId, Pair<DateTime, DateTime> timeSpan,
			BaseEventVolumeInput input, Map<TransactionKey, TransactionData> transactions) 
	{
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timeSpan.getFirst(),
			timeSpan.getSecond(), VolumeType.hits, input.pointsWanted);
		
		if (eventsMap == null) {
			return;
		}

		EventFilter eventFilter = getEventFilter(serviceId, input, timeSpan);
		
		if (eventFilter == null) {
			return;
		}
		
		for (EventResult event : eventsMap.values()) {

			if (event.entry_point == null) {
				continue;
			}
				
			TransactionData transaction = getEventTransactionData(transactions, event);
			
			if (transaction == null) {
				continue;
			}
			
			if (event.type.equals(TIMER)) {
				
				transaction.timersHits += event.stats.hits;

				if (transaction.currTimer == null) {
					transaction.currTimer = event;
				} else {
					DateTime eventFirstSeen = TimeUtil.getDateTime(event.first_seen);
					DateTime timerFirstSeen = TimeUtil.getDateTime(transaction.currTimer.first_seen);
					
					long eventDelta = timeSpan.getSecond().getMillis() - eventFirstSeen.getMillis();
					long timerDelta = timeSpan.getSecond().getMillis() - timerFirstSeen.getMillis();

					if (eventDelta < timerDelta) {
						transaction.currTimer = event;
					}				
				}	
			} else {

				if (eventFilter.filter(event)) {
					continue;
				}
				
				transaction.errorsHits += event.stats.hits;
				
				if (transaction.errors == null) {
					transaction.errors = new ArrayList<EventResult>();
				}
				
				transaction.errors.add(event);
			}
		}
	}
	
	private Map<TransactionKey, TransactionData> getTransactionDatas(Collection<TransactionGraph> transactionGraphs) {
		
		Map<TransactionKey, TransactionData> result = new HashMap<TransactionKey, TransactionData>();
				
		for (TransactionGraph transactionGraph : transactionGraphs) {	
			TransactionData transactionData = new TransactionData();	
			transactionData.graph = transactionGraph;
			Pair<String, String> pair = getTransactionNameAndMethod(transactionGraph.name, true);
			TransactionKey key = TransactionKey.of(pair.getFirst(), pair.getSecond());
			result.put(key, transactionData);
		}
		
		return result;
		
	}
	
	protected Map<TransactionKey, TransactionData> getTransactionDatas(String serviceId,
			Collection<TransactionGraph> transactionGraphs,
			Collection<TransactionGraph> baselineGraphs) {
		
		Map<TransactionKey, TransactionData> result = getTransactionDatas(transactionGraphs);
		updateTransactionPerformance(serviceId, baselineGraphs, result);
		
		return result;
	}
	
	protected TransactionDataResult getTransactionDatas(Collection<TransactionGraph> transactionGraphs,
			String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			BaseEventVolumeInput input, boolean updateEvents, int eventPoints) {
				
		if (transactionGraphs == null) {
			return null;
		}
		
		TransactionDataResult result = new TransactionDataResult();
		result.items = getTransactionDatas(transactionGraphs);
		
		if (updateEvents) {
			
			BaseEventVolumeInput eventInput;
			
			if (eventPoints != 0) {
				String json = new Gson().toJson(input);
				eventInput = new Gson().fromJson(json, input.getClass()); 
				eventInput.pointsWanted = eventPoints;
			} else {
				eventInput = input;
			}
			
			updateTransactionEvents(serviceId, timeSpan, eventInput, result.items);
		}
		
		Pair<RegressionInput, RegressionWindow> regPair = updateTransactionPerformance(serviceId, viewId, timeSpan, input, result.items);	
		
		if (regPair != null) {
			result.regressionInput = regPair.getFirst();
			result.regressionWindow = regPair.getSecond();
		}
		
		return result;

	}
	
	private static Collection<String> toArray(String value)
	{
		if (value == null) {
			return Collections.emptyList();
		}
		
		String[] types = value.split(ARRAY_SEPERATOR);
		return Arrays.asList(types);
		
	} 
	
	protected TransactionDataResult getTransactionDatas(String serviceId, Pair<DateTime, DateTime> timeSpan,
			BaseEventVolumeInput input, boolean updateEvents, int eventPoints) {
		
		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null) {
			return null;
		}
		
		Collection<TransactionGraph> transactionGraphs = getTransactionGraphs(input,
				serviceId, viewId, timeSpan, input.getSearchText(), input.pointsWanted, 0, 0);
		
		return getTransactionDatas(transactionGraphs, serviceId, viewId, timeSpan,
			input, updateEvents, eventPoints);
	}
	
	private Collection<String> getTopTransactions(String serviceId, BaseEventVolumeInput input,
		Pair<DateTime, DateTime> timespan, boolean updateEvents, TopTransactionProcessor processor) {
		
		Gson gson = new Gson();
		String json = gson.toJson(input);
		BaseEventVolumeInput cleanInput = gson.fromJson(json, input.getClass());
		cleanInput.transactions = null;
		
		TransactionDataResult transactionDataResult = getTransactionDatas(serviceId, 
			timespan, cleanInput, updateEvents, 0);
		
		Collection<String> result = processor.getTransactions(transactionDataResult);
		
		return result;
	}
	
	
	protected GroupFilter getTransactionsFilter(String serviceId, BaseEventVolumeInput input,
			Pair<DateTime, DateTime> timespan) {
		Collection<String> transactions = input.getTransactions(serviceId);
		return getTransactionsFilter(serviceId, input, timespan, transactions);
	}
		
	protected GroupFilter getTransactionsFilter(String serviceId, BaseEventVolumeInput input,
		Pair<DateTime, DateTime> timespan, Collection<String> transactions) {
		
		GroupFilter result;
		
		Collection<String> targetTransactions = new ArrayList<String>();
		
		if (transactions != null) {
			
			List<String> transactionsList = new ArrayList<String>();
			
			for (String transaction : transactions) {
				
				boolean updateEvents = false;
				TopTransactionProcessor processor;
				
				if (transaction.equals(TOP_ERRORING_TRANSACTIONS)) {
					updateEvents = true;
					processor = new TopErrorTransactionProcessor();
				} else if (transaction.equals(SLOWEST_TRANSACTIONS)) {
					processor = new TopSlowestTransactionProcessor();
				} else if (transaction.equals(SLOWING_TRANSACTIONS)) {
					processor = new TopSlowingTransactionProcessor();
				} else if (transaction.equals(HIGHEST_VOLUME_TRANSACTIONS)) {
					processor = new TopVolumeTransactionProcessor();
				} else {
					processor = null;
				}
				
				if (processor != null) {
					
					Collection<String> topTransactions = getTopTransactions(serviceId, input, 
							timespan, updateEvents, processor);
					
					if (topTransactions == null) {
						return null;
					}
					
					transactionsList.addAll(topTransactions);
				} else {
					transactionsList.add(transaction);
				}
			}
			
			targetTransactions = transactionsList;			
		} else {
			targetTransactions = transactions;
		}
						
		result = getTransactionsFilter(serviceId, targetTransactions);
		
		return result;
	}
	
	private GroupFilter getTransactionsFilter(String serviceId, Collection<String> transactions) {
		
		GroupFilter result;
		
		if (transactions != null) {
			
			GroupSettings transactionGroups = getSettings(serviceId).transactions;
			
			if (transactionGroups != null) {
				result = transactionGroups.getExpandedFilter(transactions);
			} else {
				result = GroupFilter.from(transactions);
			}
		} else {
			result = null;
		}
		
		return result;
	}
	
	protected EventFilter getEventFilter(String serviceId, BaseEventVolumeInput input, 
		Pair<DateTime, DateTime> timespan) {
		
		GroupFilter transactionsFilter = getTransactionsFilter(serviceId, input, timespan);
		
		if (transactionsFilter == null) {
			return null;
		}
		
		Categories categories = GrafanaSettings.getServiceSettings(apiClient, serviceId).getCategories();	

		
		Collection<String> allowedTypes;
		
		if (input.allowedTypes != null) {
			if (GrafanaFunction.VAR_ALL.contains(input.allowedTypes)) {
				allowedTypes = Collections.emptyList();
			} else {
				allowedTypes = toArray(input.allowedTypes);
			}
		} else {
			GeneralSettings generalSettings = getSettings(serviceId).general;
			
			if (generalSettings != null) {
				allowedTypes = generalSettings.getDefaultTypes();
			} else {
				allowedTypes = Collections.emptyList();
			}
		}
		
		Collection<String> eventLocations = VariableInput.getServiceFilters(input.eventLocations, 
			serviceId, true);
	
		return EventFilter.of(input.getTypes(apiClient, serviceId), allowedTypes, 
				input.getIntroducedBy(serviceId), eventLocations, transactionsFilter,
				input.geLabels(serviceId), input.labelsRegex, 
				input.firstSeen, categories, input.searchText, 
				input.transactionSearchText);
	}
	
	protected Collection<Transaction> getTransactions(String serviceId, String viewId,
			Pair<DateTime, DateTime> timeSpan,
			BaseEventVolumeInput input, String searchText) {
		return getTransactions(serviceId, viewId, timeSpan, input, searchText, 0, 0);	
	}
	
	protected Collection<TransactionGraph> getTransactionGraphs(BaseEventVolumeInput input, String serviceId, String viewId, 
			Pair<DateTime, DateTime> timeSpan, String searchText,
			int pointsWanted, int activeTimespan, int baselineTimespan) {
		
		GroupFilter transactionsFilter = null;
		
		if (input.hasTransactions()) {			
			transactionsFilter = getTransactionsFilter(serviceId, input, timeSpan);

			if (transactionsFilter == null) {
				return Collections.emptyList();
			}
		}
		
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
		
		if ((input.hasTransactions() || (searchText != null))) {
			result = new ArrayList<TransactionGraph>(response.data.graphs.size());
			
			for (TransactionGraph transaction : response.data.graphs) {
				Pair<String, String> nameAndMethod = getFullNameAndMethod(transaction.name);
				
				if (filterTransaction(transactionsFilter, searchText, 
					nameAndMethod.getFirst(), nameAndMethod.getSecond())) {
					continue;
				}
				
				result.add(transaction);
			}
			
		} else {
			result = response.data.graphs;
		}
		
		return result;
	}
	
	protected Collection<Transaction> getTransactions(String serviceId, String viewId,
		Pair<DateTime, DateTime> timeSpan,
		BaseEventVolumeInput input, String searchText, 
		int activeTimespan, int baselineTimespan) {
		
		Pair<String, String> fromTo = TimeUtil.toTimespan(timeSpan);
		
		TransactionsVolumeRequest.Builder builder = TransactionsVolumeRequest.newBuilder().setServiceId(serviceId)
				.setViewId(viewId).setFrom(fromTo.getFirst()).setTo(fromTo.getSecond()).setRaw(true);
		
		applyFilters(input, serviceId, builder);
		
		Response<TransactionsVolumeResult> response = ApiCache.getTransactionsVolume(apiClient, serviceId,
				input, activeTimespan, baselineTimespan, builder.build());
		
		if (response.isBadResponse()) {
			System.err.println("Transnaction volume for service " + serviceId + " code: " + response.responseCode);
		}
		
		if ((response.data == null) || (response.data.transactions == null)) {
			return null;
		}
		
		Collection<Transaction> result;
		
		if ((input.hasTransactions() || (searchText != null))) {	
			
			result = new ArrayList<Transaction>(response.data.transactions.size());
			
			GroupFilter transactionsFilter = getTransactionsFilter(serviceId, input, timeSpan);

			for (Transaction transaction : response.data.transactions) {
				Pair<String, String> nameAndMethod = getFullNameAndMethod(transaction.name);
				
				if (filterTransaction(transactionsFilter, 
					searchText, nameAndMethod.getFirst(), nameAndMethod.getSecond())) {
					continue;
				}
				
				result.add(transaction);
			}
			
		} else {
			result = response.data.transactions;
		}
		
		return result;
	}
	
	/**
	 * @param seriesName
	 *            - needed by child classes
	 * @param volumeType
	 */
	protected String getSeriesName(BaseGraphInput input, String seriesName, String serviceId,
			Collection<String> serviceIds) {
		
		return getServiceValue(input.view, serviceId, serviceIds);
	}
	
	protected void sortApplicationsByProcess(String serviceId, List<String> apps, 
		Collection<String> serversFilter, Collection<String> deploymentsFilter) {
		
		Response<JvmsResult> response =  ApiCache.getProcesses(apiClient, serviceId, true);
		
		if ((response == null) || (response.isBadResponse()) || (response.data == null) ||
			(response.data.clients == null)) {
			return;
		}
		
		Map<String, Integer> processMap = new HashMap<String, Integer>();
		
		for (Jvm jvm : response.data.clients) {
			
			if (jvm.pids == null) {
				continue;
			}
			
			if ((!CollectionUtil.safeIsEmpty(serversFilter)) &&
				(!serversFilter.contains(jvm.machine_name))) {
				continue;
			}
			
			if ((!CollectionUtil.safeIsEmpty(deploymentsFilter)) &&
			(!deploymentsFilter.contains(jvm.deployment_name))) {
					continue;
			}
			
			Integer newValue;
			Integer existingValue = processMap.get(jvm.application_name);
			
			if (existingValue != null) {
				newValue = existingValue.intValue() + jvm.pids.size();
			} else {
				newValue = jvm.pids.size();
			}
			
			processMap.put(jvm.application_name, newValue);
		}
		
		apps.sort(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				Integer a1 = processMap.get(o1);
				Integer a2 = processMap.get(o2);
				
				int i1;
				int i2;
				
				if (a1 != null) {
					i1 = a1.intValue();
				} else {
					i1 = 0;
				}
				
				if (a2 != null) {
					i2 = a2.intValue();
				} else {
					i2 = 0;
				}
				
				int delta = i2 -i1;
				
				if (delta != 0) {
					return delta;
				}
					
				return o1.compareTo(o2);
			}
		});
		
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
			int baselineWindow, int activeWindow, boolean sync) {
		
		int days = Math.abs(Days.daysBetween(from, to).getDays());
		
		int effectivePoints;
		List<SliceRequest> sliceRequests;
		
		
		if ((sync) || ((days < 3) || (days > 14))) // This is just a starting point
		{
		
			effectivePoints = pointsCount;
			sliceRequests = Collections.singletonList(new SliceRequest(from, to, pointsCount));	
		}
		else {
			effectivePoints = (pointsCount / days) + 1;
			sliceRequests = getTimeSlices(from, to, days, effectivePoints);	
		}
		
		
		List<GraphSliceTask> tasks = Lists.newArrayList();
		
		int index = 0;
		
		for (SliceRequest sliceRequest : sliceRequests) {
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
	
	protected boolean timespanContains(DateTime start, DateTime end, DateTime value) {
		
		if ((value.getMillis() > start.getMillis()) 
		&& (value.getMillis() <= end.getMillis())) {

			return true;
		}
		
		return false;
	}
	
	protected Graph mergeGraphs(Collection<GraphSliceTaskResult> graphTasks) {
		
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
				DateTime dateTime = TimeUtil.getDateTime(gp.time);
				graphPoints.put(Long.valueOf(dateTime.getMillis()), gp);
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
				try {
					taskResults.add(task.call());
				} catch (Exception e) {
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
	
	protected Pair<Map<String, EventResult>, Long> filterEvents(String serviceId, 
		Pair<DateTime, DateTime> timespan, BaseEventVolumeInput input, 
		Collection<EventResult> events) {
	
		EventFilter eventFilter = getEventFilter(serviceId, input, timespan);
		
		if (eventFilter == null) {
			return Pair.of(Collections.emptyMap(), Long.valueOf(0l));
		}
		
		long volume = 0;
		
		Map<String, EventResult> filteredEvents = new HashMap<String, EventResult>();
		
		for (EventResult event : events) {	
			
			if (eventFilter.filter(event)) {
				continue;
			}
			
			filteredEvents.put(event.id, event);
			volume += event.stats.hits;
		}
		
		Pair<Map<String, EventResult>, Long> result = Pair.of(filteredEvents, Long.valueOf(volume));		
		
		return result;
	}
	
	protected long applyGraphToEvents(Map<String, EventResult> eventListMap, 
		Graph graph, Pair<DateTime, DateTime> timespan) {
		
		long result = 0;
		
		for (GraphPoint gp : graph.points) {
			
			if (gp.contributors == null) {
				continue;
			}
			
			if (timespan != null) {
				
				DateTime gpTime = TimeUtil.getDateTime(gp.time);
				
				if (!timespanContains(timespan.getFirst(), timespan.getSecond(), gpTime)) {
					continue;
				}
			}
	
			for (GraphPointContributor gpc : gp.contributors) {		
				
				EventResult event = eventListMap.get(gpc.id);
				
				if (event != null) {
					event.stats.invocations += gpc.stats.invocations;
					event.stats.hits += gpc.stats.hits;
					result += gpc.stats.hits;
				}
			}
		}
		
		return result;
	}
		
	private Collection<EventResult> getEventListFromGraph(String serviceId, String viewId, ViewInput input,
			DateTime from, DateTime to,
			VolumeType volumeType, int pointsCount) {
		
		Graph graph = getEventsGraph(serviceId, viewId, pointsCount, input, volumeType, from, to);
		
		if (graph == null) {
			return null;
		}
		
		Collection<EventResult> events = getEventList(serviceId, viewId, input, from, to);
		
		if (events == null) {
			return null;
		}
		
		Map<String, EventResult> eventsMap = getEventsMap(events);
		appendGraphStats(eventsMap, graph);
		
		return eventsMap.values();
	}
	
	protected EventsSlimVolumeResult getEventsVolume(String serviceId, String viewId, ViewInput input, DateTime from,
		DateTime to, VolumeType volumeType) {	
		
		EventsSlimVolumeRequest.Builder builder =
				EventsSlimVolumeRequest.newBuilder().setVolumeType(volumeType).setServiceId(serviceId).setViewId(viewId)
						.setFrom(from.toString(dateTimeFormatter)).setTo(to.toString(dateTimeFormatter))
						.setVolumeType(volumeType).setRaw(true);
		
		applyBuilder(builder, serviceId, viewId, TimeUtil.toTimespan(from, to), input);
		
		Response<EventsSlimVolumeResult> response =
				ApiCache.getEventVolume(apiClient, serviceId, input, volumeType, builder.build());
		
		validateResponse(response);
		
		if ((response.data == null) || (response.data.events == null)) {
			return null;
		}
		
		return response.data;
	}
	
	private void applyVolumeToEvents(String serviceId, String viewId, ViewInput input, DateTime from, DateTime to,
			VolumeType volumeType, Map<String, EventResult> eventsMap) {
		
		EventsSlimVolumeResult eventsSlimVolumeResult = getEventsVolume(serviceId, viewId, input, from, to, volumeType);
		
		if (eventsSlimVolumeResult == null) {
			return;
		}
		
		for (EventSlimResult eventSlimResult : eventsSlimVolumeResult.events) {
			
			EventResult event = eventsMap.get(eventSlimResult.id);
			
			if (event == null) {
				continue;
			}
			
			event.stats = eventSlimResult.stats;
		}
	}
	
	private Collection<EventResult> getEventList(String serviceId, String viewId, ViewInput input, DateTime from,
			DateTime to) {
		
		EventsRequest.Builder builder = EventsRequest.newBuilder().setRaw(true);
		applyBuilder(builder, serviceId, viewId, TimeUtil.toTimespan(from, to), input);
		
		Response<?> response = ApiCache.getEventList(apiClient, serviceId, input, builder.build());
		validateResponse(response);
		
		List<EventResult> events;
		
		if (response.data instanceof EventsVolumeResult) {
			events = ((EventsVolumeResult)(response.data)).events;
		}
		else if (response.data instanceof EventsResult) {
			events = ((EventsResult)(response.data)).events;
		} else {
			return null;
		}
		
		if (events == null) {
			return null;
		}
		
		return cloneEvents(events, false);
		
	}
	
	protected Collection<EventResult> cloneEvents(Collection<EventResult> events, boolean copyStats) {
		
		List<EventResult> result = new ArrayList<EventResult>(events.size());
		
		try {
			for (EventResult event : events) {
				
				EventResult clone = (EventResult)event.clone();
				clone.stats = new Stats();
				
				if (copyStats) {
					clone.stats.hits = event.stats.hits;
					clone.stats.invocations = event.stats.invocations;
				}
				
				result.add(clone);
			}
		} catch (CloneNotSupportedException e) {
			throw new IllegalStateException(e);
			
		}
		
		return result;
	}
	
	protected static Map<String, EventResult> getEventsMap(Collection<EventResult> events) {
		return getEventsMap(events, true);
	}
	
	protected static Map<String, EventResult> getEventsMap(Collection<EventResult> events, boolean allowNoHits) {
		
		Map<String, EventResult> result = new HashMap<String, EventResult>();
		
		for (EventResult event : events) {
			if ((allowNoHits) || (event.stats.hits > 0)) {
				result.put(event.id, event);
			}
		}
		
		return result;
	}
	
	protected Map<String, EventResult> getEventMap(String serviceId, ViewInput input, DateTime from, DateTime to,
			VolumeType volumeType) {
		return getEventMap(serviceId, input, from, to, volumeType, 0);
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
				events = getEventList(serviceId, viewId, input, from, to);
				
				if (events != null) {
					applyVolumeToEvents(serviceId, viewId, input, from, to, 
						volumeType, getEventsMap(events));
				}
			}
		} else {
			events = getEventList(serviceId, viewId, input, from, to);
		}
		
		if (events == null) {
			return null;	
		}
		
		result = getEventsMap(events);
		
		return result;
	}
	
	protected List<Object> executeTasks(Collection<Callable<Object>> tasks, boolean queryPool) {	
		Executor executor;
		
		if (queryPool) {
			executor = GrafanaThreadPool.getQueryExecutor(apiClient);
		} else {
			executor  = GrafanaThreadPool.getFunctionExecutor(apiClient);
		} 
		
		CompletionService<Object> completionService = new ExecutorCompletionService<Object>(executor);
		
		for (Callable<Object> task : tasks)	{
			completionService.submit(task);
		}
		
		List<Object> result = new ArrayList<Object>();
		
		int received = 0;
		
		while (received < tasks.size()) {
			try {
				Future<Object> future = completionService.take();
				
				received++;
				Object asynResult = future.get();
				result.add(asynResult);
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		
		return result;
	}
	
	protected void applyFilters(EnvironmentsFilterInput input, String serviceId,
			TimeframeRequest.Builder builder) {
		
		for (String app : input.getApplications(apiClient, getSettings(serviceId), serviceId)) {
			builder.addApp(app);
		}
		
		for (String dep : input.getDeployments(serviceId, apiClient)) {
			builder.addDeployment(dep);
		}
		
		for (String server : input.getServers(serviceId)) {
			builder.addServer(server);
		}
	}
	
	protected SummarizedView getView(String serviceId, String viewName) {
		
		if ((viewName.length() == 0) || (viewName.startsWith(GRAFANA_VAR_PREFIX))) {
			return null;
		}
		
		ViewsRequest request = ViewsRequest.newBuilder().setServiceId(serviceId).setViewName(viewName).build();
		
		Response<ViewsResult> response = ApiCache.getView(apiClient, serviceId, viewName, request);
		
		if ((response.isBadResponse()) ||	(response.data == null) || (response.data.views == null) ||
			(response.data.views.size() == 0)) {
			return null;
		}
		
		SummarizedView result = response.data.views.get(0);
		
		return result;
	}
	
	protected static String getServiceValue(String value, String serviceId) {
		return value + SERVICE_SEPERATOR + serviceId;
	}
	
	protected static String getServiceValue(String value, String serviceId, Collection<String> serviceIds) {
		
		if (serviceIds.size() == 1) {
			return value;
		} else {
			return getServiceValue(value, serviceId);
		}
	}
	
	public static String getViewName(String name) {
	
		String result;
		
		if ((name != null) && (!name.isEmpty() && (!name.startsWith("$")))) { 
			result = name;
		} else {
			result = ALL_EVENTS;
		}
		
		return result;
		
	}
	
	protected String getViewId(String serviceId, String name) {
		
		String viewName = getViewName(name);
		
		SummarizedView view = getView(serviceId, viewName);
		
		if (view == null)
		{
			return null;
		}
		
		return view.id;
	}
	
	protected ViewInput getInput(ViewInput input) {
						
		if (!input.varTimeFilter) {
			return input;
		}
		
		Gson gson = new Gson();
		String json = gson.toJson(input);
		
		ViewInput result = gson.fromJson(json, input.getClass());
		
		if (input.timeFilter != null) {
			Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(input.timeFilter);
			result.timeFilter = TimeUtil.getTimeFilter(timespan);	
		} 
		
		return result;
		
	}
	
	protected static String formatLongValue(long value) {
		
		if (value > 1000000000) {
			return singleDigitFormatter.format(value / 1000000000.0) + "B";
		}
		
		if (value > 1000000) {
			return singleDigitFormatter.format(value / 1000000.0) + "M";
		}
		
		if (value > 1000) {
			return singleDigitFormatter.format(value / 1000.0) + "K";
		}
			
		return String.valueOf(value);
	}
	
	protected String formatMilli(Double mill) {
		return singleDigitFormatter.format(mill.doubleValue()) + "ms";
	}
	
	protected static String formatRate(double value, boolean doubleDigit) {
		
		DecimalFormat df;
		
		if (doubleDigit) {
			df = doubleDigitFormatter;
		} else {
			df = singleDigitFormatter; 
		}
		
		String result;
		String strValue = df.format(value * 100) + "%";
		
		if (strValue.startsWith("0.")) {
			result = strValue.substring(1);
		} else {
			result = strValue;
		}
		
		return result;
	} 
	
	protected Series createGraphSeries(String name, long volume) {
		return createGraphSeries(name, volume, new ArrayList<List<Object>>());
	}	
	
	protected Series createGraphSeries(String name, long volume, List<List<Object>> values) {
		
		Series result = new Series();
		
		String seriesName;

		if (volume > 0) {
			seriesName = String.format("%s (%s)", name, formatLongValue(volume));
		} else {
			seriesName = name;
		}
		
		result.name = EMPTY_NAME;
		result.columns = Arrays.asList(new String[] { TIME_COLUMN, seriesName });
		result.values = values;
		
		return result;
	}

	protected List<Series> createSingleStatSeries(Pair<DateTime, DateTime> timespan, Object singleStat) {
		
		Series series = new Series();
		
		series.name = SERIES_NAME;
		series.columns = Arrays.asList(new String[] { TIME_COLUMN, SUM_COLUMN });
		
		Long time = Long.valueOf(timespan.getSecond().getMillis());
		series.values = Collections.singletonList(Arrays.asList(new Object[] { time, singleStat }));
		
		return Collections.singletonList(series);
	}
	
	public abstract List<Series> process(FunctionInput functionInput);
}
