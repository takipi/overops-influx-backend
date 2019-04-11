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

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.Stats;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.performance.calc.PerformanceState;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.settings.RegressionSettings;
import com.takipi.api.client.util.settings.SlowdownSettings;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.input.TransactionsListInput;
import com.takipi.integrations.grafana.input.TransactionsListInput.RenderMode;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.EventLinkEncoder;
import com.takipi.integrations.grafana.util.NumberUtil;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TransactionsListFunction extends GrafanaFunction {
		
	private static final String MISSING_TIMER_LINK = "missing-timer-event";

	protected enum TransactionState {
		OK,
		WARN,
		SEVERE
	}
	
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
	
	private String getTransactionErrorDesc(TransactionData transactionData) {
		
		if (transactionData.errorsHits == 0) {
			return "No errors";
		}
		
		StringBuilder result = new StringBuilder();
		
		result.append(formatLongValue(transactionData.errorsHits));
		result.append(" errors from ");
		
		int size = Math.min(transactionData.errors.size(), 3);
		
		transactionData.errors.sort(new Comparator<EventResult>() {

			@Override
			public int compare(EventResult o1, EventResult o2) {
				return Long.compare(o2.stats.hits, o1.stats.hits);
			}
		});
		
		Map<String, Long> values = new LinkedHashMap<String, Long>(size); 
		
		
		for (int i = 0; i < size; i++) {
			
			EventResult error = transactionData.errors.get(i);
			
			if (error.error_location == null) {
				continue;
			}
				
			String key = getSimpleClassName(error.error_location.class_name);
			
			if (key == null) {
				continue;
			}
			
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
			
			if (values.size() > 1) {
				result.append("(");
				result.append(formatLongValue(entry.getValue().longValue()));
				result.append(")");
			}
			
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
		
		result.append(" in ");
		result.append(formatLongValue(transactionData.stats.invocations));
		result.append(" calls");
		
		return result.toString();	
	}

	private List<List<Object>> processServiceTransactions(String serviceId, Pair<DateTime, DateTime> timeSpan,
			TransactionsListInput input, List<String> fields, Collection<PerformanceState> states) {

		boolean updateEventBaselines = fields.contains(TransactionsListInput.ERROR_RATE_DELTA)
			|| fields.contains(TransactionsListInput.ERROR_RATE_DELTA_DESC);
		
		TransactionDataResult transactions = getTransactionDatas(serviceId, timeSpan, 
			input, true, updateEventBaselines, true);
		
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
		
		List<TransactionData> sortedTransactions = new ArrayList<TransactionData>(targets);
		
		if (fields.contains(TransactionsListInput.SLOW_STATE)) {
			sortTransactionsBySlowdowns(sortedTransactions);
		} else if (fields.contains(TransactionsListInput.ERROR_RATE_DELTA_STATE)) {
			sortTransactionsByErrorState(serviceId, sortedTransactions);
		} else {
			sortTransactionsByVolume(sortedTransactions);
		}
	
		List<List<Object>> result = formatResultObjects(sortedTransactions, 
			 serviceId, timeSpan, input, fields, transactions.regressionInput);
		
		return result;	
	}
	
	private void sortTransactionsByErrorState(String serviceId, List<TransactionData> list) {
		
		RegressionSettings regressionSettings = getSettingsData(serviceId).regression;
		
		list.sort(new Comparator<TransactionData>() {

			@Override
			public int compare(TransactionData o1, TransactionData o2) {
				
				TransactionState o1State = getErrorRateDeltaState(o1, regressionSettings);
				TransactionState o2State = getErrorRateDeltaState(o2, regressionSettings);
				
				int stateDelta = Integer.compare(o2State.ordinal(), o1State.ordinal());
				
				if (stateDelta != 0) {
					return stateDelta;
				}

				return Long.compare(o2.errorsHits, o1.errorsHits);
			}
		});
		
	}
	
	private void sortTransactionsByVolume(List<TransactionData> list) {
		
		list.sort(new Comparator<TransactionData>() {

			@Override
			public int compare(TransactionData o1, TransactionData o2) {
				
				return Long.compare(o2.stats.invocations, o1.stats.invocations);
			}
		});
		
	}
	
	private void sortTransactionsBySlowdowns(List<TransactionData> list) {
		
		list.sort(new Comparator<TransactionData>() {

			@Override
			public int compare(TransactionData o1, TransactionData o2) {
				
				int diff = o2.state.ordinal() - o1.state.ordinal();
					
				if (diff != 0) {
					return diff;
				}
				
				return Long.compare(o2.stats.invocations, o1.stats.invocations);
			}
		});
		
	}
	
	private String getTransactionMessage(TransactionData transaction) {
		
		String result = getTransactionName(transaction.graph.name, true);		
		return result;
	}
	
	private String getSlowdownDesc(TransactionData transactionData, 
		SlowdownSettings slowdownSettings, Stats stats) {
					
		StringBuilder result = new StringBuilder();
		
		boolean isSlowdown = (transactionData.state == PerformanceState.CRITICAL) ||
				(transactionData.state == PerformanceState.SLOWING);
		
		if (isSlowdown) {
			
			double baseline = transactionData.baselineStats.avg_time_std_deviation 
					* slowdownSettings.std_dev_factor + transactionData.baselineStats.avg_time;
			
			if  (transactionData.state == PerformanceState.CRITICAL) {
				result.append(TransactionState.SEVERE.toString());		
			} else {
				result.append(TransactionState.WARN.toString());		
			}
			
			result.append(": (");	
			result.append((int)(transactionData.score));
			result.append("% of calls > baseline avg ");
			result.append((int)(transactionData.baselineStats.avg_time));
			result.append("ms + ");
			result.append(slowdownSettings.std_dev_factor);
			result.append("x stdDev = ");
			result.append((int)(baseline));
			result.append("ms");
			result.append(") AND (avg response ");
			result.append((int)(stats.avg_time));
			result.append("ms - ");
			result.append((int)(transactionData.baselineStats.avg_time));
			result.append("ms baseline > ");
			result.append(slowdownSettings.min_delta_threshold);
			result.append("ms threshold)");			
		} else {
			result.append("OK: Avg response falls within baseline tolerance");
		}
	
		return result.toString();
	}
	
	private double getErrorRateDelta(TransactionData transactionData) {
		
		double result;
		
		if ((transactionData.stats.invocations > 0) 
			&& (transactionData.baselineStats != null)
			&& (transactionData.baselineStats.invocations > 0)) {
					
				double errorRate = (double)transactionData.errorsHits / (double)transactionData.stats.invocations;
				double baselineErrorRate = (double)transactionData.baselineErrors / (double)transactionData.baselineStats.invocations;
					
				result = (errorRate - baselineErrorRate) / baselineErrorRate;		
		} else {
			result = 0;
		}
				
		return result;
	}
	
	private Object formatErrorRateDelta(TransactionData transactionData) {
		
		double delta = getErrorRateDelta(transactionData);
	
		if (delta > 0.01) {
			return delta;
		}
		
		return "";
	}
	
	private Object formatErrorRateDeltaDesc(TransactionData transactionData,
			RegressionSettings regressionSettings, RegressionInput regressionInput) {
		
		StringBuilder result = new StringBuilder();

		result.append(getErrorRateDeltaState(transactionData, regressionSettings));
		
		if (transactionData.stats.invocations > 0) {
			
			result.append(": ");
			
			double errorRate = (double)transactionData.errorsHits / (double)transactionData.stats.invocations;
					
			result.append(formatLongValue(transactionData.errorsHits));
			result.append(" failures in ");
			result.append(formatLongValue(transactionData.stats.invocations));
			result.append(" calls (");
			result.append(formatRate(errorRate, true));
			result.append(")");	
			
			if ((transactionData.baselineStats != null)
			&& (transactionData.baselineStats.invocations > 0)) {
				
				double baselineErrorRate = (double)transactionData.baselineErrors / (double)transactionData.baselineStats.invocations;

				result.append(" compared to ");
				result.append(formatLongValue(transactionData.baselineErrors));
				result.append(" in ");
				result.append(formatLongValue(transactionData.baselineStats.invocations));
				result.append(" calls (");
				result.append(formatRate(baselineErrorRate, true));
				result.append(") over the last ");
				result.append(prettyTime.formatDuration(TimeUtil.now().minusMinutes(regressionInput.baselineTimespan).toDate()));
			}
		}
		
		return result.toString();
	}
	
	private Object formatErrorRate(TransactionData transactionData) {
		
		Object result;
		
		if (transactionData.stats.invocations > 0) {
			double errorRate = (double)transactionData.errorsHits / (double)transactionData.stats.invocations;
			
			if (errorRate < 0.01) {
				
				if (errorRate == 0f) {
					result = Double.valueOf(0);
				} else {
					result = "<1%";
				}
			} else {
				result = Math.min(Math.round(errorRate * 100), 100f);
			}
		} else {
			result = Double.valueOf(0);
		}
		
		return result;
	}
	
	private List<List<Object>> formatResultObjects(Collection<TransactionData> transactions, 
			String serviceId, Pair<DateTime, DateTime> timeSpan,
			TransactionsListInput input, List<String> fields,
			RegressionInput regressionInput) {
		
		SlowdownSettings slowdownSettings = getSettingsData(serviceId).slowdown;
		RegressionSettings regressionSettings = getSettingsData(serviceId).regression;
		
		List<List<Object>> result = new ArrayList<List<Object>>(transactions.size());

		for (TransactionData transactionData : transactions) {

			String name = getTransactionMessage(transactionData);
							
			String link;
			
			if (transactionData.currTimer != null) {
				link = EventLinkEncoder.encodeLink(apiClient, getSettingsData(serviceId), serviceId, input, transactionData.currTimer, 
					timeSpan.getFirst(), timeSpan.getSecond());
			} else {
				link = MISSING_TIMER_LINK;
			}
			
			Pair<Object, Object> fromTo = getTimeFilterPair(timeSpan, input.timeFilter);
			String timeRange = TimeUtil.getTimeRange(input.timeFilter); 
			
			String description = getSlowdownDesc(transactionData, slowdownSettings, transactionData.stats);
			
			Object[] object = new Object[fields.size()];
			
			setOutputField(object, fields, TransactionsListInput.LINK, link);
			setOutputField(object, fields, TransactionsListInput.TRANSACTION, name);
			setOutputField(object, fields, TransactionsListInput.TOTAL, transactionData.stats.invocations);
			setOutputField(object, fields, TransactionsListInput.AVG_RESPONSE, transactionData.stats.avg_time);
			
			setOutputField(object, fields, TransactionsListInput. BASELINE_AVG, transactionData.baselineStats.avg_time);
			setOutputField(object, fields, TransactionsListInput.BASELINE_CALLS, NumberUtil.format(transactionData.baselineStats.invocations));
			setOutputField(object, fields, TransactionsListInput.ACTIVE_CALLS, NumberUtil.format(transactionData.stats.invocations));
			
			setOutputField(object, fields, TransactionsListInput.SLOW_STATE, getPerformanceStateValue(transactionData.state));
			setOutputField(object, fields, TransactionsListInput.DELTA_DESC, description);
			
			setOutputField(object, fields, TransactionsListInput.ERRORS, transactionData.errorsHits);
			setOutputField(object, fields, TransactionsListInput.ERRORS_DESC, getTransactionErrorDesc(transactionData));
			
			setOutputField(object, fields, TransactionsListInput.ERROR_RATE, formatErrorRate(transactionData));
			setOutputField(object, fields, TransactionsListInput.ERROR_RATE_DELTA, formatErrorRateDelta(transactionData));
			setOutputField(object, fields, TransactionsListInput.ERROR_RATE_DELTA_DESC, formatErrorRateDeltaDesc(transactionData, regressionSettings, regressionInput));
			setOutputField(object, fields, TransactionsListInput.ERROR_RATE_DELTA_STATE, formatErrorRateDeltaState(transactionData, regressionSettings));

			
			setOutputField(object, fields, ViewInput.FROM, fromTo.getFirst());
			setOutputField(object, fields, ViewInput.TO, fromTo.getSecond());
			setOutputField(object, fields, ViewInput.TIME_RANGE, timeRange);
			
			result.add(Arrays.asList(object));
		}

		return result;
	}

	private int getPerformanceStateValue(PerformanceState state) {
		
		if (state == null) {
			return TransactionState.OK.ordinal();
		}
		
		switch (state) {
			
			case SLOWING:
				return TransactionState.WARN.ordinal();
		
			case CRITICAL:
				return TransactionState.SEVERE.ordinal();
	
			default: 
				return TransactionState.OK.ordinal();
		}
	}
	
	private TransactionState getErrorRateDeltaState(TransactionData transactionData, RegressionSettings regressionSettings) {
		
		if (transactionData.stats.invocations < regressionSettings.error_min_volume_threshold) {
			return TransactionState.OK;
		}
		
		if (transactionData.stats.invocations > 0) { 
			double errorRate = (double)transactionData.errorsHits / (double)transactionData.stats.invocations;
			
			if (errorRate < 0.01) {
				return TransactionState.OK;	
			}	
		} else {
			return TransactionState.OK;	
		}
		
		double delta = getErrorRateDelta(transactionData);
		
		if (delta > regressionSettings.error_critical_regression_delta) {
			return TransactionState.SEVERE;
		}
		
		if (delta > regressionSettings.error_regression_delta) {
			return TransactionState.WARN;
		}
		
		return TransactionState.OK;
	}
	
	private Object formatErrorRateDeltaState(TransactionData transactionData, RegressionSettings regressionSettings) {
	
		TransactionState state = getErrorRateDeltaState(transactionData, regressionSettings);
		return state.ordinal();
	}
	
	private void setOutputField(Object[] target, List<String> fields, String field, Object value) {
		
		int index = fields.indexOf(field);
		
		if (index != -1) {
			
			if ((value instanceof Double) && (Double.isNaN((Double)value))) {
				target[index] = Double.valueOf(0);
			} else {
				target[index] = value;
			}
		}
	}
			
	private int getServiceSingleStat(String serviceId, TransactionsListInput input, 
		Pair<DateTime, DateTime> timeSpan, Collection<PerformanceState> states) {	
		
		boolean updateBaseline = states.size() < PerformanceState.values().length;
		
		TransactionDataResult transactionDataResult = getTransactionDatas(serviceId,
			timeSpan, input, false, updateBaseline, true);

		if (transactionDataResult == null) {
			return 0;
		}
		
		int result = 0;
	
		for (TransactionData transactionData : transactionDataResult.items.values()) {
			
			if ((!updateBaseline) || (states.contains(transactionData.state))) {
				result++;
			}
		}
		
		return result;
	}
	
	private int getSingleStat(Collection<String> serviceIds, 
		TransactionsListInput input, Pair<DateTime, DateTime> timeSpan,
		Collection<PerformanceState> states) {
		
		int result = 0;
		
		for (String serviceId : serviceIds) {
			result += getServiceSingleStat(serviceId, input, timeSpan, states);
		}
		
		return result;
	}
	
	private String getSingleStatDesc(Collection<String> serviceIds, 
			TransactionsListInput input, Pair<DateTime, DateTime> timeSpan,
			Collection<PerformanceState> states) {
			
		StringBuilder result = new StringBuilder();
			
		for (String serviceId : serviceIds) {				
			
			TransactionDataResult transactionDataResult = getTransactionDatas(serviceId,
					timeSpan, input, false, false, true);

			String serviceDesc = getSlowdownsDesc(transactionDataResult.items.values(), 
				states, RegressionsInput.MAX_TOOLTIP_ITEMS);
			
			if (serviceIds.size() > 1) {
				result.append(serviceId);
				result.append(" = ");
			}
			
			result.append(serviceDesc);
			
			if (serviceIds.size() > 1) {
				result.append(". ");
			}
		}
			
		return result.toString();
	}
	
	private List<Series> processSingleStatBaselineAvg(TransactionsListInput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds) {
		
		if (CollectionUtil.safeIsEmpty(serviceIds)) {
			return Collections.emptyList();
		}
						
		long baselineInvocations = 0;
		
		Collection<PerformanceState> states = TransactionsListInput.getStates(input.performanceStates);
				
		Map<String, TransactionDataResult> transactionServiceMap = new HashMap<String, TransactionDataResult>();
		
		for (String serviceId : serviceIds) {
			
			TransactionDataResult transactionDataResult = getTransactionDatas(serviceId,
				timeSpan, input, false, false,  true);

			if (transactionDataResult == null) {
				continue;
			}
			
			transactionServiceMap.put(serviceId, transactionDataResult);
			
			for (TransactionData transactionData : transactionDataResult.items.values()) {
				
				if (states.contains(transactionData.state)) {
					baselineInvocations += transactionData.baselineStats.invocations;
				}
			}	
		}
		
		if (baselineInvocations == 0) {
			return createSingleStatSeries(timeSpan, formatMilli((double)0));
		}

		double avg = 0;
		
		for (String serviceId : serviceIds) {
					
			TransactionDataResult transactionDataResult = transactionServiceMap.get(serviceId);

			if (transactionDataResult == null) {
				continue;
			}
				
			for (TransactionData transactionData : transactionDataResult.items.values()) {
					
				double baselineAvg = transactionData.baselineStats.avg_time;
				
				if ((states.contains(transactionData.state) && (!Double.isNaN(baselineAvg)))) {
					avg += baselineAvg * transactionData.baselineStats.invocations / baselineInvocations;
				}
			}		
		}
		
		return createSingleStatSeries(timeSpan, formatMilli(avg));
	}
	
	private List<Series> processSingleStatAvg(TransactionsListInput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds) {
		
		if (CollectionUtil.safeIsEmpty(serviceIds)) {
			return Collections.emptyList();
		}
						
		long volume = 0;
		
		Collection<PerformanceState> states = TransactionsListInput.getStates(input.performanceStates);
		boolean updateBaseline = states.size() < PerformanceState.values().length;
	
		for (String serviceId : serviceIds) {
			
			TransactionDataResult transactionDataResult = getTransactionDatas(serviceId,
				timeSpan, input, false, updateBaseline, true);

			if (transactionDataResult == null) {
				continue;
			}
			
			for (TransactionData transactionData : transactionDataResult.items.values()) {
				if ((!updateBaseline) || (states.contains(transactionData.state))) {
					volume += transactionData.stats.invocations;
				}
			}	
		}
		
		if (volume == 0) {
			return createSingleStatSeries(timeSpan, formatMilli((double)0));
		}

		double avg = 0;
		
		for (String serviceId : serviceIds) {
					
			TransactionDataResult transactionDataResult = getTransactionDatas(serviceId,
				timeSpan, input, false, false, true);

			if (transactionDataResult == null) {
				continue;
			}
				
			for (TransactionData transactionData : transactionDataResult.items.values()) {
					
				if (states.contains(transactionData.state)) {
					avg += transactionData.stats.avg_time * transactionData.stats.invocations / volume;
				}
			}		
		}
		
		return createSingleStatSeries(timeSpan, formatMilli(avg));
	}
	
	private List<Series> processSingleStatVolume(TransactionsListInput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds) {
		
		if (CollectionUtil.safeIsEmpty(serviceIds)) {
			return Collections.emptyList();
		}
						
		long volume = 0;
		
		Collection<PerformanceState> states = TransactionsListInput.getStates(input.performanceStates);
		
		boolean updateBaseline = states.size() < PerformanceState.values().length;
		
		for (String serviceId : serviceIds) {
			
			TransactionDataResult transactionDataResult = getTransactionDatas(serviceId,
				timeSpan, input, false, updateBaseline, true);

			if (transactionDataResult == null) {
				continue;
			}
				
			for (TransactionData transactionData : transactionDataResult.items.values()) {
					
				if ((!updateBaseline) || (states.contains(transactionData.state))) {
					volume += transactionData.stats.invocations;
				}
			}		
		}
		
		return createSingleStatSeries(timeSpan, formatLongValue(volume));
	}
	
	private List<Series> processSingleStat(TransactionsListInput input, Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds) {
				
		if (CollectionUtil.safeIsEmpty(serviceIds)) {
			return Collections.emptyList();
		}
						
		Collection<PerformanceState> performanceStates = TransactionsListInput.getStates(input.performanceStates);
		
		Object singleStatText;
		int singleStatValue = getSingleStat(serviceIds, input, timeSpan, performanceStates);
		
		if (input.singleStatFormat != null) {
			if (singleStatValue > 0) {
				singleStatText = String.format(input.singleStatFormat, String.valueOf(singleStatValue));
			} else {
				singleStatText = EMPTY_POSTFIX;
			}
		} else {
			singleStatText = Integer.valueOf(singleStatValue);
		}
		
		
		return createSingleStatSeries(timeSpan, singleStatText);
	}

	private List<Series> processSingleStatDesc(TransactionsListInput input, 
		Pair<DateTime, DateTime> timeSpan, Collection<String> serviceIds) {
		
		if (CollectionUtil.safeIsEmpty(serviceIds)) {
			return Collections.emptyList();
		}
						
		Collection<PerformanceState> performanceStates = TransactionsListInput.getStates(input.performanceStates);
		
		String singleStatDesc = getSingleStatDesc(serviceIds, input, timeSpan, performanceStates);
		
		return createSingleStatSeries(timeSpan, singleStatDesc);
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
		
		List<List<List<Object>>> servicesValues = new ArrayList<List<List<Object>>>(serviceIds.size());

		for (String serviceId : serviceIds) {
			
			List<List<Object>> serviceEvents = processServiceTransactions(serviceId, 
				timeSpan, input, series.columns, performanceStates);		
			
			series.values.addAll(serviceEvents);
			servicesValues.add(serviceEvents);
		}
		
		sortSeriesValues(series.values, servicesValues);


		return Collections.singletonList(series);
	}
	
	private static String getSlowdownRateStr(TransactionData transactionData) {
		return Math.round(getSlowdownRate(transactionData) * 100) + "%";
	}
	
	private static double getSlowdownRate(TransactionData transactionData) {
		return transactionData.stats.avg_time / transactionData.baselineStats.avg_time;
	}
	
	public static String getSlowdownsDesc(Collection<TransactionData> transactionDatas,
		Collection<PerformanceState> states, int maxSize) {
		
		StringBuilder result = new StringBuilder();
					
		List<TransactionData> slowdowns = new ArrayList<TransactionData>();
		List<TransactionData> severeSlowdowns = new ArrayList<TransactionData>();
		
		int slowdownsSize = 0;
		
		for (TransactionData transactionData : transactionDatas) {
			
			if ((states != null) && (!states.contains(transactionData.state))) {
				continue;
			}
			
			if (transactionData.state == PerformanceState.OK) {
				continue;
			}
			
			slowdownsSize++;
			
			if (severeSlowdowns.size() + slowdowns.size() >= maxSize) {
				continue;
			}
						
			if (transactionData.state == PerformanceState.CRITICAL) {
				severeSlowdowns.add(transactionData);
			} else if (transactionData.state == PerformanceState.SLOWING) {
				slowdowns.add(transactionData);	
			}			
		}
		
		int index = 0;
		
		Collection<TransactionData> sortedSevereSlowdowns = getTransactionSortedByDelta(severeSlowdowns);
	
		for (TransactionData transactionData : sortedSevereSlowdowns) {
		
			result.append("+");
			result.append(getSlowdownRateStr(transactionData));
			result.append(" ");
			result.append(getTransactionName(transactionData.graph.name, false));
			result.append("(P1)");			
			index++;
			
			if ((index < severeSlowdowns.size()) || (slowdowns.size() > 0)) {
				result.append(", ");
			}
		}
		
		index = 0;
		
		Collection<TransactionData> sortedSlowdowns = getTransactionSortedByDelta(slowdowns);
		
		for (TransactionData transactionData : sortedSlowdowns) {
			
			result.append("+");
			result.append(getSlowdownRateStr(transactionData));
			result.append(" ");
			result.append(getTransactionName(transactionData.graph.name, false));
		
			index++;
		
			if (index < slowdowns.size()) {
				result.append(", ");
			}
		}
		
		int remaining = slowdowns.size() + severeSlowdowns.size() - slowdownsSize;
		
		if (remaining > 0) {
			result.append(" and ");
			result.append(remaining);
			result.append(" more");
		}
		
		return result.toString();
	}
	
	private static Collection<TransactionData> getTransactionSortedByDelta(Collection<TransactionData> transactionDatas) {
		
		List<TransactionData> result = new  ArrayList<TransactionData>(transactionDatas);
		
		result.sort(new Comparator<TransactionData>() {

			@Override
			public int compare(TransactionData o1, TransactionData o2) {
				double r1 = getSlowdownRate(o1);
				double r2 = getSlowdownRate(o2);

				if (r2 > r1) {
					return 1;
				}
				
				if (r2 < r1) {
					return -1;
				}
				
				return 0;
			}
		});
		
		return result;
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof TransactionsListInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		TransactionsListInput input = (TransactionsListInput)getInput((ViewInput)functionInput);

		if (input.renderMode == null) {
			throw new IllegalStateException("Missing render mode");
		}
		
		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		Collection<String> serviceIds = getServiceIds(input);
		
		RenderMode renderMode = input.getRenderMore();
		
		switch (renderMode) {
			
			case Grid:
				return processGrid(input, timeSpan, serviceIds);
				
			case SingleStatDesc:
				return processSingleStatDesc(input, timeSpan, serviceIds);
			
			case SingleStatVolume:
				return processSingleStatVolume(input, timeSpan, serviceIds);
				
			case SingleStat:
				return processSingleStat(input, timeSpan, serviceIds);
				
			case SingleStatAvg:
				return processSingleStatAvg(input, timeSpan, serviceIds);
				
			case SingleStatBaselineAvg:
				return processSingleStatBaselineAvg(input, timeSpan, serviceIds);
										
			default:
				throw new IllegalStateException(String.valueOf(renderMode));
			
		}
	}
}
