package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

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

	public TransactionsListFunction(ApiClient apiClient) {
		super(apiClient);
	}

	protected class TransactionData {
		protected TransactionGraph graph;
		protected TransactionGraph baselineGraph;
		protected long timersHits;
		protected long errorsHits;
		protected EventResult currTimer;
		protected PerformanceState state;
		protected double score;
		protected double baselineAvg;
		protected long baselineInvocations;

	}

	private List<List<Object>> processServiceTransactions(String serviceId, Pair<DateTime, DateTime> timeSpan,
			TransactionsListInput input, List<String> fields, Collection<PerformanceState> states) {

		Map<String, TransactionData> transactions = getTransactionDatas(serviceId, timeSpan, 
			input, true, input.eventPointsWanted);
		
		if (transactions == null) {
			return Collections.emptyList();
		}
		
		Collection<TransactionData> targets;
		
		if (input.performanceStates != null) {
			List<TransactionData> matchingTransactions = new ArrayList<TransactionData>(transactions.size());
			
			for (TransactionData transactionData : transactions.values()) {
				if (states.contains(transactionData.state)) {
					matchingTransactions.add(transactionData);
				}
			}
			targets = matchingTransactions;
		} else {
			targets = transactions.values();
		}	

		List<List<Object>> result = formatResultObjects(targets, serviceId, timeSpan, input, fields);
		
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
	
	private String getSlowdownDesc(TransactionData transactionData, SlowdownSettings slowdownSettings ) {
		
		if (transactionData.baselineAvg == 0) {
			return "";
		}
		
		if (transactionData.score == 0) {
			return "";
		}
		
		Stats stats = TransactionUtil.aggregateGraph(transactionData.baselineGraph);
		double baseline = stats.avg_time_std_deviation * slowdownSettings.std_dev_factor + transactionData.baselineAvg;
		
		StringBuilder result = new StringBuilder();
		
		result.append((int)(transactionData.score));
		result.append("% of calls took more than baseline avg + ");
		result.append(slowdownSettings.std_dev_factor);
		result.append("x std dev (");
		result.append(decimalFormat.format(baseline));
		result.append("ms)");
		
		if ((transactionData.state == PerformanceState.CRITICAL) ||
			(transactionData.state == PerformanceState.SLOWING)) {
			result.append(" and avg response > baseline by ");
			result.append(slowdownSettings.min_delta_threshold);
			result.append("ms");			
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
			Stats stats = getTrasnactionGraphStats(transactionData.graph);
					
			double errorRate;
			
			if (stats.invocations > 0) {
				errorRate = (double)transactionData.errorsHits / (double)stats.invocations;
			} else {
				errorRate = 0;
			}
				
			String link;
			
			if (transactionData.currTimer != null) {
				link = EventLinkEncoder.encodeLink(apiClient, serviceId, input, transactionData.currTimer, 
					timeSpan.getFirst(), timeSpan.getSecond());
			} else {
				link = null;
			}
			
			Pair<Object, Object> fromTo = getTimeFilterPair(timeSpan, input.timeFilter);
			String timeRange = TimeUtil.getTimeUnit(input.timeFilter); 

			int scoreValue = getStateValue(transactionData.state);
		
			Object[] object = new Object[fields.size()];
			
			setOutputObjectField(object, fields, TransactionsListInput.LINK, link);
			setOutputObjectField(object, fields, TransactionsListInput.TRANSACTION, name);
			setOutputObjectField(object, fields, TransactionsListInput.TOTAL, stats.invocations);
			setOutputObjectField(object, fields, TransactionsListInput.AVG_RESPONSE, stats.avg_time);
			setOutputObjectField(object, fields, TransactionsListInput. BASELINE_AVG, transactionData.baselineAvg);
			setOutputObjectField(object, fields, TransactionsListInput.BASELINE_CALLS, NumberUtil.format(transactionData.baselineInvocations));
			setOutputObjectField(object, fields, TransactionsListInput.ACTIVE_CALLS, NumberUtil.format(stats.invocations));
			setOutputObjectField(object, fields, TransactionsListInput.SLOW_STATE, getStateValue(transactionData.state));
			setOutputObjectField(object, fields, TransactionsListInput.SLOW_DELTA, scoreValue);
			setOutputObjectField(object, fields, TransactionsListInput.DELTA_DESC, getSlowdownDesc(transactionData, slowdownSettings));
			setOutputObjectField(object, fields, TransactionsListInput.ERROR_RATE, errorRate);
			setOutputObjectField(object, fields, TransactionsListInput.ERRORS, transactionData.errorsHits);
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
	
	private TransactionData getTransactionData(Map<String, TransactionData> transactions, EventResult event) {
		
		TransactionData result = transactions.get(event.entry_point.class_name);

		if (result == null) {
			
			result = transactions.get(toTransactionName(event.entry_point));
			
			if (result == null) {
				return null;
			}
		}
		
		return result;
	}
	
	public void updateTransactionPerformance(String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			BaseEventVolumeInput input, Map<String, TransactionData> transactionDatas) {
	
		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		Pair<RegressionInput, RegressionWindow> inputPair = regressionFunction.getRegressionInput(serviceId, viewId, input, timeSpan);
		
		if (inputPair == null) {
			return;
		}
		
		RegressionInput regressionInput = inputPair.getFirst();
		RegressionWindow regressionWindow = inputPair.getSecond();
		
		DateTime baselineStart = regressionWindow.activeWindowStart.minusMinutes(regressionInput.baselineTimespan);
		
		BaseEventVolumeInput baselineInput;
		
		if (input.hasDeployments()) {
			Gson gson = new Gson();
			String json = gson.toJson(input);
			baselineInput = gson.fromJson(json, input.getClass());
			baselineInput.deployments = null;
		} else {
			baselineInput = input;
		}
		
		Collection<TransactionGraph> baselineTransactionGraphs = getTransactionGraphs(baselineInput, serviceId, 
				viewId, Pair.of(baselineStart, regressionWindow.activeWindowStart), 
				null, baselineInput.pointsWanted, 
				regressionWindow.activeTimespan, regressionInput.baselineTimespan);
			
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
			
			TransactionData transactionData = transactionDatas.get(toQualified(transactionName));
			
			if (transactionData == null) {
				continue;
			}
			
			transactionData.state = performanceScore.state;
			transactionData.score = performanceScore.score;
						
			transactionData.baselineGraph = baselineGraphsMap.get(transactionName);
		
			if (transactionData.baselineGraph != null) {
				Stats baselineStats = getTrasnactionGraphStats(transactionData.baselineGraph);
				
				if (baselineStats != null) {
					transactionData.baselineAvg = baselineStats.avg_time;
					transactionData.baselineInvocations = baselineStats.invocations;
				}
			}
		}
	}
	
	
	private void updateTransactionEvents(String serviceId, Pair<DateTime, DateTime> timeSpan,
			BaseEventVolumeInput input, Map<String, TransactionData> transactions) 
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
				
			TransactionData transaction = getTransactionData(transactions, event);
			
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
			}
		}
	}
	
	public Map<String, TransactionData> getTransactionDatas(String serviceId, Pair<DateTime, DateTime> timeSpan,
			BaseEventVolumeInput input, boolean updateEvents, int eventPoints) {
		
		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null) {
			return Collections.emptyMap();
		}
		
		Collection<TransactionGraph> transactionGraphs = getTransactionGraphs(input,
				serviceId, viewId, timeSpan, input.getSearchText(), input.pointsWanted, 0, 0);
		
		return getTransactionDatas(transactionGraphs, serviceId, viewId, timeSpan,
			input, updateEvents, eventPoints);
	}
	
	public Map<String, TransactionData> getTransactionDatas(Collection<TransactionGraph> transactionGraphs,
			String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			BaseEventVolumeInput input, boolean updateEvents, int eventPoints) {
				
		if (transactionGraphs == null) {
			return Collections.emptyMap();
		}
				
		Map<String, TransactionData> result = new HashMap<String, TransactionData>();

		for (TransactionGraph transactionGraph :transactionGraphs) {	
			TransactionData transactionData = new TransactionData();	
			transactionData.graph = transactionGraph;
			result.put(toQualified(transactionGraph.name), transactionData);
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
			
			updateTransactionEvents(serviceId, timeSpan, eventInput, result);
		}
		
		updateTransactionPerformance(serviceId, viewId, timeSpan, input, result);	
		
		return result;

	}
	
	private int getServiceSingleStat(String serviceId, TransactionsListInput input, 
		Pair<DateTime, DateTime> timeSpan, Collection<PerformanceState> states)
	{	
		int result = 0;

		Map<String, TransactionData> transactionDatas = getTransactionDatas(serviceId,
			timeSpan, input, false, 0);

		for (TransactionData transactionData : transactionDatas.values()) {
			
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
