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
import com.takipi.integrations.grafana.input.EventFilterInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsListIput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.SlowdownSettings;
import com.takipi.integrations.grafana.util.EventLinkEncoder;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TransactionsListFunction extends GrafanaFunction {
	
	private static final String LINK = "link";
	private static final String TRANSACTION = "transaction";
	private static final String TOTAL = "invocations";
	private static final String AVG_RESPONSE = "avg_response";
	private static final String SLOW_STATE = "slow_state";
	private static final String SLOW_DELTA = "slow_delta";
	private static final String ERROR_RATE = "error_rate";
	private static final String ERRORS = "errors";
	private static final String DELTA_DESC = "delta_description";

	
	private static final List<String> FIELDS = Arrays.asList(new String[] { 
			LINK, TRANSACTION, TOTAL, AVG_RESPONSE, SLOW_STATE, SLOW_DELTA,
			DELTA_DESC, ERROR_RATE, ERRORS });

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsListFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionsListIput.class;
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
		protected long timersHits;
		protected long errorsHits;
		protected EventResult currTimer;
		protected PerformanceState state;
		protected double score;
		protected double baselineAvg;
	}

	private List<List<Object>> processServiceTransactions(String serviceId, Pair<DateTime, DateTime> timeSpan,
			TransactionsListIput input, List<String> fields, Collection<PerformanceState> states) {

		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null) {
			return Collections.emptyList();
		}
		
		Map<String, TransactionData> transactions = getTransactionDatas(serviceId, viewId, timeSpan, input);
		
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
		
		String result;
		String transactionName = getTransactionName(transaction.graph.name, true);
		
		if (transaction.currTimer != null) {
						
			/*
			if (transaction.currTimer.message != null) {
				int index = transaction.currTimer.message.indexOf(TIMER_MESSAGE);
				
				if (index != -1) {
					result = transactionName + " > " + transaction.currTimer.message.substring(index + TIMER_MESSAGE.length());
				} else {
					if (!transaction.currTimer.message.contains(transactionName)) {
						result = transactionName + ": " + transaction.currTimer.message;	
					} else {
						result = transaction.currTimer.message.replace(PARALLAX_TIMER_POSTFIX, "");
					}
				}	
			}
			 else {
			*/
				result = transactionName;
			//}
		} else {
			result = transactionName;
		}
		
		return result;
	}
	
	private Stats getTrasnactionGraphStats(TransactionGraph graph) {
		
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
	
	private String getSlowdownDesc(TransactionData transactionData, double stddevFactor) {
		
		if (transactionData.baselineAvg == 0) {
			return "";
		}
		
		if (transactionData.score == 0) {
			return "";
		}
		
		StringBuilder result = new StringBuilder();
		
		result.append((int)(transactionData.score));
		result.append("% of calls took more than baseline avg ");
		result.append((int)(transactionData.baselineAvg));
		result.append(" + ");
		result.append(stddevFactor);
		result.append(" std deviations");
		
		return result.toString();
	}
	
	private List<List<Object>> formatResultObjects(Collection<TransactionData> transactions, 
			String serviceId, Pair<DateTime, DateTime> timeSpan,
			TransactionsListIput input, List<String> fields) {
		
		double stddevFactor = GrafanaSettings.getData(apiClient, serviceId).slowdown.std_dev_factor;
		
		List<List<Object>> result = new ArrayList<List<Object>>(transactions.size());

		for (TransactionData transaction : transactions) {

			String name = getTransactionMessage(transaction);
			Stats stats = getTrasnactionGraphStats(transaction.graph);
					
			double errorRate;
			
			if (stats.invocations > 0) {
				errorRate = (double)transaction.errorsHits / (double)stats.invocations;
			} else {
				errorRate = 0;
			}
				
			String link;
			
			if (transaction.currTimer != null) {
				link = EventLinkEncoder.encodeLink(apiClient, serviceId, input, transaction.currTimer, 
					timeSpan.getFirst(), timeSpan.getSecond());
			} else {
				link = null;
			}
			
			Pair<Object, Object> fromTo = getTimeFilterPair(timeSpan, input.timeFilter);
			
			Object[] object = new Object[fields.size()];
			
			setOutputObjectField(object, fields, LINK, link);
			setOutputObjectField(object, fields, TRANSACTION, name);
			setOutputObjectField(object, fields, TOTAL, stats.invocations);
			setOutputObjectField(object, fields, AVG_RESPONSE, stats.avg_time);
			setOutputObjectField(object, fields, SLOW_STATE, getStateValue(transaction.state));
			setOutputObjectField(object, fields, SLOW_DELTA, transaction.score);
			setOutputObjectField(object, fields, DELTA_DESC, getSlowdownDesc(transaction, stddevFactor));
			setOutputObjectField(object, fields, ERROR_RATE, errorRate);
			setOutputObjectField(object, fields, ERRORS, transaction.errorsHits);
			setOutputObjectField(object, fields, FROM, fromTo.getFirst());
			setOutputObjectField(object, fields, TO, fromTo.getSecond());

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
	
	private void updateTransactionPerformance(String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			EventFilterInput input, Map<String, TransactionData> transactionDatas) {
	
		RegressionFunction regressionFunction = new RegressionFunction(apiClient);
		Pair<RegressionInput, RegressionWindow> inputPair = regressionFunction.getRegressionInput(serviceId, viewId, input, timeSpan);
		
		if (inputPair == null) {
			return;
		}
		
		RegressionInput regressionInput = inputPair.getFirst();
		RegressionWindow regressionWindow = inputPair.getSecond();
		
		DateTime baselineStart = regressionWindow.activeWindowStart.minusMinutes(regressionInput.baselineTimespan);
		
		EventFilterInput baselineInput;
		
		if (input.hasDeployments()) {
			Gson gson = new Gson();
			String json = gson.toJson(input);
			baselineInput = gson.fromJson(json, EventFilterInput.class);
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
			
			TransactionGraph baselineGraph = baselineGraphsMap.get(transactionName);
			
			if (baselineGraph != null) {
				Stats baselineStats = getTrasnactionGraphStats(baselineGraph);
				
				if (baselineStats != null) {
					transactionData.baselineAvg = baselineStats.avg_time;
				}
			}
		}
	}
	
	
	private void updateTransactionEvents(String serviceId, Pair<DateTime, DateTime> timeSpan,
			EventFilterInput input, Map<String, TransactionData> transactions) 
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
			EventFilterInput input) {
		
		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null) {
			return Collections.emptyMap();
		}
		
		return getTransactionDatas(serviceId, viewId, timeSpan, input);
	}
	
	public Map<String, TransactionData> getTransactionDatas(String serviceId, String viewId, Pair<DateTime, DateTime> timeSpan,
			EventFilterInput input) {
				
		Collection<TransactionGraph> transactionGraphs = getTransactionGraphs(input,
			serviceId, viewId, timeSpan, input.getSearchText(), input.pointsWanted, 0, 0);

		if (transactionGraphs == null) {
			return Collections.emptyMap();
		}
				
		Map<String, TransactionData> result = new HashMap<String, TransactionData>();

		for (TransactionGraph transactionGraph :transactionGraphs) {	
			TransactionData transactionData = new TransactionData();	
			transactionData.graph = transactionGraph;
			result.put(toQualified(transactionGraph.name), transactionData);
		}
		
		updateTransactionEvents(serviceId, timeSpan, input, result);
		updateTransactionPerformance(serviceId, viewId, timeSpan, input, result);
		
		
		return result;

	}
	
	private int getServiceSingleStat(String serviceId, TransactionsListIput input, 
		Pair<DateTime, DateTime> timeSpan, Collection<PerformanceState> states)
	{	
		int result = 0;

		Map<String, TransactionData> transactionDatas = getTransactionDatas(serviceId, timeSpan, input);

		for (TransactionData transactionData : transactionDatas.values()) {
			
			if (states.contains(transactionData.state)) {
				result++;
			}
		}
		
		return result;
	}
	
	private int getSingleStat(Collection<String> serviceIds, 
		TransactionsListIput input, Pair<DateTime, DateTime> timeSpan,
		Collection<PerformanceState> states)
	{
		
		int result = 0;
		
		for (String serviceId : serviceIds)
		{
			result += getServiceSingleStat(serviceId, input, timeSpan, states);
		}
		
		return result;
	}
	
	private Collection<PerformanceState> getStates(TransactionsListIput input) {
		
		List<PerformanceState> result = new ArrayList<PerformanceState>();
		
		if (input.performanceStates != null) {
			String[] parts = input.performanceStates.split(ARRAY_SEPERATOR);
			
			for (String part : parts) {
				PerformanceState state = PerformanceState.valueOf(part);
				
				if (state == null) {
					throw new IllegalStateException("Unsupported state " + part + " in " + input.performanceStates);
				}
				
				result.add(state);
			}
		} else {
			for (PerformanceState state : PerformanceState.values()) {
				result.add(state);
			}
		}
		
		return result;
	}

	private List<Series> processSingleStat(TransactionsListIput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds) {
				
		if (CollectionUtil.safeIsEmpty(serviceIds))
		{
			return Collections.emptyList();
		}
						
		Object singleStatText;
		int singleStatValue = getSingleStat(serviceIds, input, timeSpan, getStates(input));
		
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

	private List<Series> processGrid(TransactionsListIput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds) {
		
		Series series = new Series();
		
		series.name = SERIES_NAME;
		
		if (input.fields != null) {
			series.columns = Arrays.asList(input.fields.split(ARRAY_SEPERATOR));
		} else {
			series.columns = FIELDS;
		}
		series.values = new ArrayList<List<Object>>();

		for (String serviceId : serviceIds) {
			List<List<Object>> serviceEvents = processServiceTransactions(serviceId, 
				timeSpan, input, series.columns, getStates(input));		
			series.values.addAll(serviceEvents);
		}

		return Collections.singletonList(series);
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput)
	{
		
		if (!(functionInput instanceof TransactionsListIput)) {
			throw new IllegalArgumentException("functionInput");
		}

		TransactionsListIput input = (TransactionsListIput) functionInput;
		
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
