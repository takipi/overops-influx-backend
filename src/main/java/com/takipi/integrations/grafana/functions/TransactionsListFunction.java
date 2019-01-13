package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.Stats;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.performance.PerformanceUtil;
import com.takipi.api.client.util.performance.calc.PerformanceCalculator;
import com.takipi.api.client.util.performance.calc.PerformanceScore;
import com.takipi.api.client.util.performance.calc.PerformanceState;
import com.takipi.api.client.util.performance.transaction.GraphPerformanceCalculator;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.api.client.util.transaction.TransactionUtil;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseEventVolumeInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsListInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.input.SlowdownSettings;
import com.takipi.integrations.grafana.util.EventLinkEncoder;
import com.takipi.integrations.grafana.util.NumberUtil;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TransactionsListFunction extends GrafanaFunction {
		
	private static final String MISSING_TIMER_LINK = "missing-timer-event";
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsListFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionsListInput.class;
		}

		@Override
		public String getName() {
			return "transactionsList";
		}
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
		public boolean equals(Object obj)
		{
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
		public int hashCode()
		{
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

	protected class TransactionData {
		protected Stats stats;
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
	
	public TransactionsListFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	private String getTransactionErrorDesc(TransactionData transactionData) {
		
		if (transactionData.errorsHits == 0) {
			return "No errors";
		}
		
		StringBuilder result = new StringBuilder();
		
		result.append(transactionData.errorsHits);
		result.append(" errors in ");
		
		int size = Math.min(transactionData.errors.size(), 3);
		
		transactionData.errors.sort(new Comparator<EventResult>()
		{

			@Override
			public int compare(EventResult o1, EventResult o2)
			{
				return (int)(o2.stats.hits - o1.stats.hits);
			}
		});
		
		Map<String, Long> values = new LinkedHashMap<String, Long>(size); 
		
		
		for (int i = 0; i < size; i++) {
			EventResult error = transactionData.errors.get(i);
			String key = getSimpleClassName(error.error_location.class_name);
			
			Long existing = values.get(key);
			
			if (existing != null) {
				values.put(key, existing.longValue() + error.stats.hits);
			} else {
				values.put(key, error.stats.hits);
			}
			
		}
		
		int index = 0;
		
		for (Map.Entry<String, Long> entry : values.entrySet()) {
			
			result.append(entry.getKey());
			
			result.append("(");
			result.append(entry.getValue());
			result.append(")");
			
			if (index < values.size() - 1) {
				result.append(", ");
			}
			
			index++;
		}
		
		if (transactionData.errors.size() - size > 0) {
			result.append(" and ");
			result.append(transactionData.errors.size() - size );
			result.append(" more locations");
		}
			
		return result.toString();	
	}

	private List<List<Object>> processServiceTransactions(String serviceId, Pair<DateTime, DateTime> timeSpan,
			TransactionsListInput input, List<String> fields, Collection<PerformanceState> states) {

		TransactionDataResult transactions = getTransactionDatas(serviceId, timeSpan, 
			input, true, input.eventPointsWanted);
		
		if (transactions == null) {
			return Collections.emptyList();
		}
		
		Collection<TransactionData> targets;
		
		if (input.performanceStates != null) {
			List<TransactionData> matchingTransactions = new ArrayList<TransactionData>(transactions.items.size());
			
			for (TransactionData transactionData : transactions.items.values()) {
				if (states.contains(transactionData.state)) {
					matchingTransactions.add(transactionData);
				}
			}
			targets = matchingTransactions;
		} else {
			targets = transactions.items.values();
		}	
		
		List<TransactionData> sorted = new ArrayList<TransactionData>(targets);
		
		sorted.sort(new Comparator<TransactionData>()
		{

			@Override
			public int compare(TransactionData o1, TransactionData o2)
			{
				int diff = o2.state.ordinal() - o1.state.ordinal();
				
				if (diff != 0) {
					return diff;
				}
				
				return (int)(o2.stats.invocations - o1.stats.invocations);
			}
		});

		List<List<Object>> result = formatResultObjects(sorted, serviceId, timeSpan, input, fields);
		
		return result;	
	}
	
	private String getTransactionMessage(TransactionData transaction) {
		
		String result = getTransactionName(transaction.graph.name, true);		
		return result;
	}
	
	private static Stats getTrasnactionGraphStats(TransactionGraph graph) {
		
		Stats result = new Stats();
		
		if (graph.points == null) {
			return result;
		}
				
		for (GraphPoint gp : graph.points) {
			if (gp.stats != null) {
				result.invocations += gp.stats.invocations;
			}
		}
		
		double avgTimeSum = 0;
		
		for (GraphPoint gp : graph.points) {
			if (gp.stats != null) {
				avgTimeSum += gp.stats.avg_time * gp.stats.invocations;
			}
		}
		
		result.avg_time = avgTimeSum / result.invocations;
		
		
		return result;
	}
	
	private String getSlowdownDesc(TransactionData transactionData, 
		SlowdownSettings slowdownSettings, Stats stats) {
				
		Stats baselineStats = TransactionUtil.aggregateGraph(transactionData.baselineGraph);
		double baseline = baselineStats.avg_time_std_deviation * slowdownSettings.std_dev_factor + transactionData.baselineAvg;
		
		StringBuilder result = new StringBuilder();
		
		boolean isSlowdown = (transactionData.state == PerformanceState.CRITICAL) ||
				(transactionData.state == PerformanceState.SLOWING);
		
		if (isSlowdown) {
			
			if  (transactionData.state == PerformanceState.CRITICAL) {
				result.append("Slow: ");		
			} else {
				result.append("Slowing: ");		
			}
			
			result.append("(");	
			result.append((int)(transactionData.score));
			result.append("% of calls > baseline avg ");
			result.append((int)(baselineStats.avg_time));
			result.append("ms + ");
			result.append(slowdownSettings.std_dev_factor);
			result.append("x stdDev = ");
			result.append((int)(baseline));
			result.append("ms");
			result.append(") AND (avg response ");
			result.append((int)(stats.avg_time));
			result.append("ms - ");
			result.append((int)(baselineStats.avg_time));
			result.append("ms baseline > ");
			result.append(slowdownSettings.min_delta_threshold);
			result.append("ms threshold)");			
		} else {
			result.append("OK: Avg response falls within baseline tolerance");
		}
	
		return result.toString();
	}
	
	private List<List<Object>> formatResultObjects(Collection<TransactionData> transactions, 
			String serviceId, Pair<DateTime, DateTime> timeSpan,
			TransactionsListInput input, List<String> fields) {
		
		SlowdownSettings slowdownSettings = GrafanaSettings.getData(apiClient, serviceId).slowdown;
		
		List<List<Object>> result = new ArrayList<List<Object>>(transactions.size());

		for (TransactionData transactionData : transactions) {

			String name = getTransactionMessage(transactionData);
			
			double errorRate;
			
			if (transactionData.stats.invocations > 0) {
				errorRate = (double)transactionData.errorsHits / (double)transactionData.stats.invocations;
			} else {
				errorRate = 0;
			}
				
			String link;
			
			if (transactionData.currTimer != null) {
				link = EventLinkEncoder.encodeLink(apiClient, serviceId, input, transactionData.currTimer, 
					timeSpan.getFirst(), timeSpan.getSecond());
			} else {
				link = MISSING_TIMER_LINK;
			}
			
			Pair<Object, Object> fromTo = getTimeFilterPair(timeSpan, input.timeFilter);
			String timeRange = TimeUtil.getTimeRange(input.timeFilter); 
			
			String description = getSlowdownDesc(transactionData, slowdownSettings, transactionData.stats);
			
			Object[] object = new Object[fields.size()];
			
			setOutputObjectField(object, fields, TransactionsListInput.LINK, link);
			setOutputObjectField(object, fields, TransactionsListInput.TRANSACTION, name);
			setOutputObjectField(object, fields, TransactionsListInput.TOTAL, transactionData.stats.invocations);
			setOutputObjectField(object, fields, TransactionsListInput.AVG_RESPONSE, transactionData.stats.avg_time);
			setOutputObjectField(object, fields, TransactionsListInput. BASELINE_AVG, transactionData.baselineAvg);
			setOutputObjectField(object, fields, TransactionsListInput.BASELINE_CALLS, NumberUtil.format(transactionData.baselineInvocations));
			setOutputObjectField(object, fields, TransactionsListInput.ACTIVE_CALLS, NumberUtil.format(transactionData.stats.invocations));
			setOutputObjectField(object, fields, TransactionsListInput.SLOW_STATE, getStateValue(transactionData.state));
			setOutputObjectField(object, fields, TransactionsListInput.DELTA_DESC, description);
			setOutputObjectField(object, fields, TransactionsListInput.ERROR_RATE, errorRate);
			setOutputObjectField(object, fields, TransactionsListInput.ERRORS, transactionData.errorsHits);
			setOutputObjectField(object, fields, TransactionsListInput.ERRORS_DESC, getTransactionErrorDesc(transactionData));

			setOutputObjectField(object, fields, ViewInput.FROM, fromTo.getFirst());
			setOutputObjectField(object, fields, ViewInput.TO, fromTo.getSecond());
			setOutputObjectField(object, fields, ViewInput.TIME_RANGE, timeRange);
			

			result.add(Arrays.asList(object));
		}

		return result;
	}

	private int getStateValue(PerformanceState state) {
		
		if (state == null) {
			return 0;
		}
		
		switch (state) {
			
			case SLOWING:
				return 1;
		
			case CRITICAL:
				return 2;
	
			default: 
				return 0;
		}
	}
	
	private void setOutputObjectField(Object[] target, List<String> fields, String field, Object value) {
		
		int index = fields.indexOf(field);
		
		if (index != -1) {
			target[index] = value;
		}
	}
	
	private TransactionData getEventTransactionData(Map<TransactionKey, TransactionData> transactions, EventResult event) {
		
		TransactionKey classOnlyKey = TransactionKey.of(event.entry_point.class_name, null);
		TransactionData result = transactions.get(classOnlyKey);

		if (result == null) {
			
			TransactionKey classAndMethodKey = TransactionKey.of(event.entry_point.class_name, 
				event.entry_point.method_name);
			
			result = transactions.get(classAndMethodKey);
		}
		
		return result;
	}
	
	/**
	 * @param timeSpan - needed for children 
	 */
	protected Collection<TransactionGraph> getBaselineTransactionGraphs(
		String serviceId, String viewId, BaseEventVolumeInput input, 
		Pair<DateTime, DateTime> timeSpan, RegressionInput regressionInput, 
		RegressionWindow regressionWindow) {
		
		BaseEventVolumeInput baselineInput;
		
		if (input.hasDeployments()) {
			Gson gson = new Gson();
			String json = gson.toJson(input);
			baselineInput = gson.fromJson(json, input.getClass());
			baselineInput.deployments = null;
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
	
	private Pair<RegressionInput, RegressionWindow> updateTransactionPerformance(String serviceId, String viewId, 
			Pair<DateTime, DateTime> timeSpan, BaseEventVolumeInput input, 
			Map<TransactionKey, TransactionData> transactionDatas) {
	
		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		Pair<RegressionInput, RegressionWindow> result = regressionFunction.getRegressionInput(serviceId, viewId, input, timeSpan);
		
		if (result == null) {
			return null;
		}
		
		RegressionInput regressionInput = result.getFirst();
		RegressionWindow regressionWindow = result.getSecond();
		
		Collection<TransactionGraph> baselineTransactionGraphs = getBaselineTransactionGraphs(serviceId, 
			viewId, input, timeSpan, regressionInput, regressionWindow);
	
		SlowdownSettings slowdownSettings = GrafanaSettings.getData(apiClient, serviceId).slowdown;
		
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
				Stats baselineStats = getTrasnactionGraphStats(transactionData.baselineGraph);
				
				if (baselineStats != null) {
					transactionData.baselineAvg = baselineStats.avg_time;
					transactionData.baselineInvocations = baselineStats.invocations;
				}
			}
		}
		
		return result;
	}
	
	
	private void updateTransactionEvents(String serviceId, Pair<DateTime, DateTime> timeSpan,
			BaseEventVolumeInput input, Map<TransactionKey, TransactionData> transactions) 
	{
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timeSpan.getFirst(),
			timeSpan.getSecond(), VolumeType.hits, input.pointsWanted);
		
		if (eventsMap == null) {
			return;
		}

		EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);
		
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
					DateTime evrntFirstSeen = TimeUtil.getDateTime(event.first_seen);
					DateTime timerFirstSeen = TimeUtil.getDateTime(transaction.currTimer.first_seen);
					
					long eventDelta = timeSpan.getSecond().getMillis() - evrntFirstSeen.getMillis();
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
	
	public TransactionDataResult getTransactionDatas(String serviceId, Pair<DateTime, DateTime> timeSpan,
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
			
	public TransactionDataResult getTransactionDatas(Collection<TransactionGraph> transactionGraphs,
			String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			BaseEventVolumeInput input, boolean updateEvents, int eventPoints) {
				
		if (transactionGraphs == null) {
			return null;
		}
		
		TransactionDataResult result = new TransactionDataResult();
		result.items = new HashMap<TransactionKey, TransactionData>();
				
		for (TransactionGraph transactionGraph :transactionGraphs) {	
			TransactionData transactionData = new TransactionData();	
			transactionData.graph = transactionGraph;
			Pair<String, String> pair = getTransactionNameAndMethod(transactionGraph.name, true);
			TransactionKey key = TransactionKey.of(pair.getFirst(), pair.getSecond());
			result.items.put(key, transactionData);
		}
		
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
	
	private int getServiceSingleStat(String serviceId, TransactionsListInput input, 
		Pair<DateTime, DateTime> timeSpan, Collection<PerformanceState> states)
	{	
		TransactionDataResult transactionDataResult = getTransactionDatas(serviceId,
			timeSpan, input, false, 0);

		if (transactionDataResult == null) {
			return 0;
		}
		
		int result = 0;
	
		for (TransactionData transactionData : transactionDataResult.items.values()) {
			
			if (states.contains(transactionData.state)) {
				result++;
			}
		}
		
		return result;
	}
	
	private int getSingleStat(Collection<String> serviceIds, 
		TransactionsListInput input, Pair<DateTime, DateTime> timeSpan,
		Collection<PerformanceState> states)
	{
		
		int result = 0;
		
		for (String serviceId : serviceIds)
		{
			result += getServiceSingleStat(serviceId, input, timeSpan, states);
		}
		
		return result;
	}
	
	private List<Series> processSingleStat(TransactionsListInput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds) {
				
		if (CollectionUtil.safeIsEmpty(serviceIds))
		{
			return Collections.emptyList();
		}
						
		Collection<PerformanceState> performanceStates = TransactionsListInput.getStates(input.performanceStates);
		
		Object singleStatText;
		int singleStatValue = getSingleStat(serviceIds, input, timeSpan, performanceStates);
		
		if (input.singleStatFormat != null)
		{
			if (singleStatValue > 0)
			{
				singleStatText = String.format(input.singleStatFormat, String.valueOf(singleStatValue));
			}
			else
			{
				singleStatText = EMPTY_POSTFIX;
			}
		}
		else
		{
			singleStatText = Integer.valueOf(singleStatValue);
		}
		
		
		return createSingleStatSeries(timeSpan, singleStatText);
	}

	private List<Series> processGrid(TransactionsListInput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds) {
		
		Series series = new Series();
		
		series.name = SERIES_NAME;
		
		if (input.fields != null) {
			series.columns = Arrays.asList(input.fields.split(ARRAY_SEPERATOR));
		} else {
			series.columns = TransactionsListInput.FIELDS;
		}
		series.values = new ArrayList<List<Object>>();

		Collection<PerformanceState> performanceStates = TransactionsListInput.getStates(input.performanceStates);
		
		for (String serviceId : serviceIds) {
			List<List<Object>> serviceEvents = processServiceTransactions(serviceId, 
				timeSpan, input, series.columns, performanceStates);		
			series.values.addAll(serviceEvents);
		}

		return Collections.singletonList(series);
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput)
	{
		
		if (!(functionInput instanceof TransactionsListInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		TransactionsListInput input = (TransactionsListInput) functionInput;
		
		if (input.renderMode == null)
		{
			throw new IllegalStateException("Missing render mode");
		}
		
		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		Collection<String> serviceIds = getServiceIds(input);
		
		switch (input.renderMode)
		{
			case Grid:
				return processGrid(input, timeSpan, serviceIds);
				
			default:
				return processSingleStat(input, timeSpan, serviceIds);
			
		}
	}
}
