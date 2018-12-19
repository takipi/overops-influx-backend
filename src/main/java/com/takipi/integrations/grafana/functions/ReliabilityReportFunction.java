package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.result.event.EventSlimResult;
import com.takipi.api.client.result.event.EventsSlimVolumeResult;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.performance.calc.PerformanceState;
import com.takipi.api.client.util.regression.RateRegression;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.functions.TransactionsListFunction.TransactionData;
import com.takipi.integrations.grafana.input.EventFilterInput;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.input.RelabilityReportInput;
import com.takipi.integrations.grafana.input.RelabilityReportInput.GraphType;
import com.takipi.integrations.grafana.input.RelabilityReportInput.ReportMode;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.RegressionReportSettings;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.DeploymentUtil;
import com.takipi.integrations.grafana.util.TimeUtil;

public class ReliabilityReportFunction extends EventsFunction {
	private static final Logger logger = LoggerFactory.getLogger(ReliabilityReportFunction.class);
	
	private static final List<String> FIELDS = Arrays.asList(
			new String[] { FROM, TO, TIME_RANGE, "Name", 
					"NewIssuesDesc", "RegressionsDesc", "SlowdownsDesc", 
					"NewIssuesState", "RegressionsState", "SlowdownsState", 
					"Score" });
	
	private enum IssueTypeStatus {
		GREEN, YELLOW, RED
	}
	
	public static class Factory implements FunctionFactory {
		
		@Override
		public GrafanaFunction create(ApiClient apiClient)
		{
			return new ReliabilityReportFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass()
		{
			return RelabilityReportInput.class;
		}
		
		@Override
		public String getName()
		{
			return "regressionReport";
		}
	}
	
	protected static class ReportKeyOutput {
		protected String key;
		protected RegressionData regressionData;
		protected Collection<TransactionData> transactionDatas;
		
		protected ReportKeyOutput(String key) {
			this.key = key;
		}
	}
	
	protected static class ReportKeyResults {
		protected ReportKeyOutput output;
		protected int newIssues;
		protected int severeNewIssues;
		protected int criticalRegressions;
		protected int regressions;
		protected int slowdowns;
		protected int severeSlowdowns;
		protected double score;
		
		protected ReportKeyResults(ReportKeyOutput output) {
			this.output = output;	
		}
	}
	
	protected static class ReportAsyncResult {
		String key;
		
		protected ReportAsyncResult(String key) {
			this.key = key;
		}
	}
	
	protected static class RegressionAsyncResult extends ReportAsyncResult {
		protected RegressionOutput output;
		
		protected RegressionAsyncResult(String key, RegressionOutput output) {
			super(key);
			this.output = output;
		}
	}
	
	protected static class SlowdownAsyncResult extends ReportAsyncResult{
		protected Collection<TransactionData> transactionDatas;
		
		protected SlowdownAsyncResult(String key, Collection<TransactionData> transactionDatas) {
			super(key);
			this.transactionDatas = transactionDatas;
		}
	}

	public static class SlowdownAsyncTask extends BaseAsyncTask implements Callable<Object> {

		protected TransactionsListFunction function;
		protected String key;
		protected String serviceId;
		protected EventFilterInput input;
		protected Pair<DateTime, DateTime> timeSpan;

		protected SlowdownAsyncTask(TransactionsListFunction function, String key, String serviceId, RegressionsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			this.function = function;
			this.key = key;
			this.serviceId = serviceId;
			this.input = input;
			this.timeSpan = timeSpan;
		}

		@Override
		public Object call() {

			beforeCall();

			try {

				Map<String, TransactionData> tansactionDatas = function.getTransactionDatas(serviceId, timeSpan, input);
				SlowdownAsyncResult result = new SlowdownAsyncResult(key, tansactionDatas.values());
				
				return result;
			} finally {
				afterCall();
			}

		}

		@Override
		public String toString() {
			return String.join(" ", "Regression", serviceId, key);
		}
	}
	
	public static class RegressionAsyncTask extends BaseAsyncTask implements Callable<Object> {

		protected RegressionFunction function;
		protected String key;
		protected String serviceId;
		protected RegressionsInput input;
		protected Pair<DateTime, DateTime> timeSpan;

		protected RegressionAsyncTask(RegressionFunction function, String key, String serviceId, RegressionsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			this.function = function;
			this.key = key;
			this.serviceId = serviceId;
			this.input = input;
			this.timeSpan = timeSpan;
		}

		@Override
		public Object call() {

			beforeCall();

			try {

				RegressionOutput output = function.runRegression(serviceId, input); 
				RegressionAsyncResult result = new RegressionAsyncResult(key, output);
				
				return result;
			} finally {
				afterCall();
			}

		}

		@Override
		public String toString() {
			return String.join(" ", "Regression", serviceId, key);
		}
	}

	protected static class VolumeOutput {
		
		protected String key;
		protected long volume;
		
		protected VolumeOutput(String key) {
			this.key = key;
		}
	}

	protected class AppVolumeAsyncTask extends BaseAsyncTask implements Callable<Object> {

		protected String app;
		protected String serviceId;
		protected RegressionsInput input;
		protected Pair<DateTime, DateTime> timeSpan;

		protected AppVolumeAsyncTask(RegressionsInput input, String serviceId, String app,
				Pair<DateTime, DateTime> timeSpan) {

			this.app = app;
			this.serviceId = serviceId;
			this.timeSpan = timeSpan;
			this.input = input;
		}

		@Override
		public Object call() {

			beforeCall();

			try {
				
				long volume = 0;
				String viewId = getViewId(serviceId, input.view);
				
				if (viewId != null)
				{
					EventsSlimVolumeResult eventsVolume = getEventsVolume(serviceId, viewId, input, timeSpan.getFirst(),
							timeSpan.getSecond(), VolumeType.hits);

					if (eventsVolume != null) {
						for (EventSlimResult eventResult : eventsVolume.events) {
							volume += eventResult.stats.hits;
						}
					}
				}		

				VolumeOutput result = new VolumeOutput(app);
				result.volume = volume;

				return result;
			} finally {
				afterCall();
			}

		}

		@Override
		public String toString() {
			return String.join(" ", "App Volume", serviceId, app);
		}

	}

	public ReliabilityReportFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private RelabilityReportInput getInput(RelabilityReportInput reportInput, String name) {

		if (reportInput.mode == null) {
			return reportInput;
		}
		
		Gson gson = new Gson();
		String json = gson.toJson(reportInput);
		RelabilityReportInput result = gson.fromJson(json, RelabilityReportInput.class);

		switch (reportInput.mode) {
		
			case Deployments:
				result.deployments = name;
				break;
				
			case Tiers:
				result.types = EventFilter.CATEGORY_PREFIX + name;
				break;
			
			case Applications:
				result.applications = name;
				break;	
		}

		return result;
	}

	@Override
	protected List<String> getColumns(String fields) {
		return FIELDS;
	}

	private Collection<String> getRoutingVolumes(String serviceId, RelabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan) {
 		
		Collection<String> tiers = GrafanaSettings.getServiceSettings(apiClient, serviceId).getTierNames();
		
		Map<String, VolumeOutput> volumeMap = new HashMap<String, VolumeOutput>();
		
		if (tiers != null) {

			if (tiers.size() >= input.limit) {
				return tiers;
			}
			
			for (String tier : tiers) {
				volumeMap.put(tier, new VolumeOutput(tier));
			}	
		} 
		
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timeSpan.getFirst(), 
			timeSpan.getSecond(), VolumeType.hits);
		
		if (eventsMap == null) {
			return Collections.emptyList();
		}
		
		Categories categories = GrafanaSettings.getServiceSettings(apiClient, serviceId).getCategories();
				
		for (EventResult event : eventsMap.values()) {
			
			Set<String> labels = new HashSet<String>();
			
			if (event.error_origin != null) {
				
				Set<String> originlabels = categories.getCategories(event.error_origin.class_name);
				
				if (originlabels != null) {
					labels.addAll(originlabels);
				}
			}
			
			if (event.error_location != null) {
				
				Set<String> locationLabels = categories.getCategories(event.error_location.class_name);
				
				if (locationLabels != null) {
					labels.addAll(locationLabels);
				}
			}
			
			for (String label : labels) {
				VolumeOutput volumeOutput = volumeMap.get(label);
				
				if (volumeOutput == null) {
					volumeOutput = new VolumeOutput(label);
					volumeMap.put(label, volumeOutput);
				}
				
				volumeOutput.volume += event.stats.hits;
			}		
		}
		
		Collection<String> result = limitVolumes(new ArrayList<VolumeOutput>(volumeMap.values()), 
			input.limit);

		return result;
	}

	private List<VolumeOutput> getAppVolumes(String serviceId, RelabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan, Collection<String> apps) {

		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();

		for (String app : apps) {
			tasks.add(new AppVolumeAsyncTask(getInput(input, app), serviceId, app, timeSpan));
		}

		List<VolumeOutput> result = new ArrayList<VolumeOutput>();
		List<Object> taskResults = executeTasks(tasks, true);

		for (Object taskResult : taskResults) {

			if (taskResult instanceof VolumeOutput) {
				result.add((VolumeOutput) taskResult);
			}
		}

		return result;
	}

	protected static class RegressionData {

		protected String key;
		RateRegression regression;
		RegressionOutput regressionOutput;

		protected RegressionData(RateRegression regression, String key,
				RegressionOutput regressionOutput) {

			this.key = key;
			this.regression = regression;
			this.regressionOutput = regressionOutput;
		}
	}

	protected List<ReportAsyncResult> processAsync(String serviceId, RelabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan, Collection<String> keys) {

		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
		List<ReportAsyncResult> result = new ArrayList<ReportAsyncResult>();
				
		for (String key : keys) {
			
			RegressionFunction regressionFunction = new RegressionFunction(apiClient);
			TransactionsListFunction transactionsListFunction = new TransactionsListFunction(apiClient);

			RegressionsInput regressionsInput = getInput(input, key);
			RegressionsInput transactionInput = getInput(input, key);

			transactionInput.pointsWanted = input.transactionPointsWanted;
			
			RegressionOutput regressionOutput = ApiCache.getRegressionOutput(apiClient, serviceId, regressionsInput, regressionFunction, false);
					
			if (regressionOutput != null) {
				result.add(new RegressionAsyncResult(key, regressionOutput));
			} else {
				tasks.add(new RegressionAsyncTask(regressionFunction, key, serviceId, regressionsInput, timeSpan));
			}
			
			if (input.mode != ReportMode.Tiers) {
				tasks.add(new SlowdownAsyncTask(transactionsListFunction, 
					key, serviceId, transactionInput, timeSpan));
			}
		}
		
		if (tasks.size() == 1) {
			try {
				result.add((ReportAsyncResult)(tasks.get(0).call()));
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		} else {
			List<Object> taskResults = executeTasks(tasks, false);
	
			for (Object taskResult : taskResults) {
	
				if (taskResult instanceof ReportAsyncResult) {
					result.add((ReportAsyncResult) taskResult);
				}
			}
		}

		return result;
	}

	private Collection<String> limitVolumes(List<VolumeOutput> volumes, int limit) {

		volumes.sort(new Comparator<VolumeOutput>() {

			@Override
			public int compare(VolumeOutput o1, VolumeOutput o2) {

				return (int) (o2.volume - o1.volume);
			}
		});

		List<String> result = new ArrayList<String>();

		for (int i = 0; i < Math.min(limit, volumes.size()); i++) {
			result.add(volumes.get(i).key);
		}

		return result;
	}

	private Collection<String> getActiveApplications(String serviceId, RelabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan) {

		Collection<String> apps = ClientUtil.getApplications(apiClient, serviceId, true);

		if (apps.size() < input.limit) {
			return apps;
		}

		List<VolumeOutput> appVolumes = getAppVolumes(serviceId, input, timeSpan, apps);

		Collection<String> result = limitVolumes(appVolumes, input.limit);

		return result;
	}

	private static void sortDeployments(List<String> deps) {
		deps.sort(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return DeploymentUtil.compareDeployments(o1, o2);
			}
		});

	}

	private Collection<String> getActiveDeployments(String serviceId, RelabilityReportInput input) {

		List<String> activeDeps = ClientUtil.getDeployments(apiClient, serviceId, true);

		sortDeployments(activeDeps);

		List<String> result = new ArrayList<String>();

		for (int i = 0; i < Math.min(input.limit, activeDeps.size()); i++) {
			result.add(activeDeps.get(i));
		}

		if (input.limit - result.size() > 0) {
			List<String> nonActiveDeps = ClientUtil.getDeployments(apiClient, serviceId, false);
			sortDeployments(nonActiveDeps);

			for (int i = 0; i < nonActiveDeps.size(); i++) {
				String dep = nonActiveDeps.get(i);
				
				if (!result.contains(dep)) {
					result.add(dep);
				}
				
				if (result.size() >= input.limit) {
					break;
				}
			}
		}

		return result;
	}
	
	

	private Collection<String> getActiveKeys(String serviceId, RelabilityReportInput regInput,
			Pair<DateTime, DateTime> timeSpan) {
		
		Collection<String> keys;
		
		if (regInput.mode != null) {
			switch (regInput.mode) {
				
				case Deployments:
					keys = getActiveDeployments(serviceId, regInput);
					break;
					
				case Tiers: 
					keys = getRoutingVolumes(serviceId, regInput, timeSpan);
					break;
				
				case Applications: 
					keys = getActiveApplications(serviceId, regInput, timeSpan);
					break;
					
				default: 
					throw new IllegalStateException("Unsopported mode " + regInput.mode);
			}
		} else {
			keys = Collections.singleton("");
		}

		return keys;
	}
	
	public static double getScore(RegressionReportSettings reportSettings, RegressionOutput regressionOutput,
		int slowdowns, int severeslowdowns)
	{
		
		int newEventsScore = regressionOutput.newIssues * reportSettings.new_event_score;
		int severeNewEventScore = regressionOutput.severeNewIssues * reportSettings.severe_new_event_score;
		int criticalRegressionsScore = (regressionOutput.criticalRegressions + severeslowdowns) * reportSettings.critical_regression_score;
		int regressionsScore = (regressionOutput.regressions + slowdowns) * reportSettings.regression_score;
		
		long days = Math.max(1, TimeUnit.MINUTES.toDays(regressionOutput.regressionInput.activeTimespan));			
		double score = (newEventsScore + severeNewEventScore + criticalRegressionsScore + regressionsScore) / days;
		double weightedScore = Math.max(100 - (reportSettings.score_weight * score), 0);
		
		return weightedScore;
	}	
	
	protected Collection<ReportKeyOutput> executeReport(String serviceId, RelabilityReportInput regInput,
			Pair<DateTime, DateTime> timeSpan) {
		
		Collection<String> keys = getActiveKeys(serviceId, regInput, timeSpan);
		
		logger.debug("Executing report " + regInput.mode + " keys: " + Arrays.toString(keys.toArray()));

		List<ReportAsyncResult> AsyncResults = processAsync(serviceId, regInput, timeSpan, keys);

		Map<String, ReportKeyOutput> reportKeyOutputMap = new HashMap<String, ReportKeyOutput>();
		
		for (ReportAsyncResult asyncResult : AsyncResults) {

			ReportKeyOutput reportKeyOutput = reportKeyOutputMap.get(asyncResult.key);
			
			if (reportKeyOutput == null) {
				reportKeyOutput = new ReportKeyOutput(asyncResult.key);
				reportKeyOutputMap.put(asyncResult.key, reportKeyOutput);
			}
			
			if (asyncResult instanceof RegressionAsyncResult) {
			
				RegressionOutput regressionOutput = ((RegressionAsyncResult)asyncResult).output;
	
				if ((regressionOutput == null) || (regressionOutput.rateRegression == null)) {
					continue;
				}
				
				RegressionData regressionData = new RegressionData(regressionOutput.rateRegression, asyncResult.key, 
						regressionOutput);
				
				reportKeyOutput.regressionData = regressionData;
			}
			
			if (asyncResult instanceof SlowdownAsyncResult) {
				
				Collection<TransactionData> TransactionDatas = ((SlowdownAsyncResult)asyncResult).transactionDatas;
				reportKeyOutput.transactionDatas = TransactionDatas;
			}
		}

		Collection<ReportKeyOutput> result;
		
		if (ReportMode.Deployments.equals(regInput.mode)) {
			List<ReportKeyOutput> sorted = new ArrayList<ReportKeyOutput>(reportKeyOutputMap.values());
			sortDeployments(sorted, true);
			result = sorted;
		} else {
			
			List<ReportKeyOutput> sorted = new ArrayList<ReportKeyOutput>(reportKeyOutputMap.values());
			sortKeys(sorted, true);
			result = sorted;
			
			//result =  reportKeyOutputMap.values();
		}
		
		return result;
	}
	
	private void sortDeployments(List<ReportKeyOutput> scores, boolean asc) {
		scores.sort(new Comparator<ReportKeyOutput>() {

			@Override
			public int compare(ReportKeyOutput o1, ReportKeyOutput o2) {

				if (asc) {
					return DeploymentUtil.compareDeployments(o1.key, o2.key);
				} else {
					return DeploymentUtil.compareDeployments(o2.key, o1.key);
				}
			}
		});
	}
	
	private void sortKeys(List<ReportKeyOutput> scores, boolean asc) {
		scores.sort(new Comparator<ReportKeyOutput>() {

			@Override
			public int compare(ReportKeyOutput o1, ReportKeyOutput o2) {

				if (asc) {
					return o1.key.compareTo(o2.key);
				} else {
					return o2.key.compareTo(o1.key);
				}
			}
		});
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof RelabilityReportInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		RelabilityReportInput input = (RelabilityReportInput) functionInput;

		if (input.render == null) {
			throw new IllegalStateException("Missing render mode");
		}
		
		switch (input.render) {
			
			case Graph:
				return processGraph(input);
			case Grid:
				return super.process(input);	
			case SingleStat:
				return processSingleStat(input);	
			default:
				throw new IllegalStateException("Unsupported render mode " + input.render); 
		}
	}
	
	private double getServiceSingleStat(String serviceId, Pair<DateTime, DateTime> timeSpan, RelabilityReportInput input)
	{
		Collection<ReportKeyOutput> reportKeyOutputs = executeReport(serviceId, input, timeSpan);
		Collection<ReportKeyResults> reportKeyResults = getReportResults(serviceId, reportKeyOutputs);
		
		double result;
		
		if (reportKeyResults.size() > 0) {
			ReportKeyResults reportKeyResult = reportKeyResults.iterator().next();
			result = reportKeyResult.score;
		} else {
			result = 0;
		}
		
		return result;
	}
	
	private double getSingleStat(Collection<String> serviceIds, Pair<DateTime, DateTime> timeSpan, RelabilityReportInput input)
	{
		
		double result = 0;
		
		for (String serviceId : serviceIds)
		{
			result += getServiceSingleStat(serviceId, timeSpan, input);
		}
		
		return result;
	}

	private List<Series> processSingleStat(RelabilityReportInput input)
	{
		Collection<String> serviceIds = getServiceIds(input);
		
		if (CollectionUtil.safeIsEmpty(serviceIds))
		{
			return Collections.emptyList();
		}
		
		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		
		Object singleStatText = getSingleStat(serviceIds, timeSpan, input);
		
		return createSingleStatSeries(timeSpan, singleStatText);
	}

	private static Object formatIssueDesc(RelabilityReportInput input, int nonSevere, int severe)
	{
		
		Object result;
		
		if (severe > 0)
		{
			if ((nonSevere == 0) || (nonSevere == severe))
			{
				if (input.sevOnlyFormat != null) {
					result = String.format(input.sevOnlyFormat, severe);
				} else {
					result = String.valueOf(severe);
				}
			}
			else
			{
				if (input.sevAndNonSevFormat != null) {
					result = String.format(input.sevAndNonSevFormat, nonSevere + severe, severe);
				} else {
					result = String.valueOf(nonSevere + severe);
				}
			}
		}
		else
		{
			result = Integer.valueOf(nonSevere);
		}
		
		return result;
	}
	
	private Collection<ReportKeyResults> getReportResults(String serviceId, Collection<ReportKeyOutput> reportKeyOutputs) {
		
		RegressionReportSettings reportSettings = GrafanaSettings.getData(apiClient, serviceId).regression_report;
		
		if (reportSettings == null)
		{
			throw new IllegalStateException("Unable to acquire regression report settings for " + serviceId);
		}			
		
		List<ReportKeyResults> result = new ArrayList<ReportKeyResults>();
		
		for (ReportKeyOutput reportKeyOutput : reportKeyOutputs) {			
			
			ReportKeyResults reportKeyResults = new ReportKeyResults(reportKeyOutput);
		
			if (reportKeyOutput.regressionData != null) {
				reportKeyResults.newIssues = reportKeyOutput.regressionData.regressionOutput.newIssues;
				reportKeyResults.severeNewIssues = reportKeyOutput.regressionData.regressionOutput.severeNewIssues;
				reportKeyResults.criticalRegressions = reportKeyOutput.regressionData.regressionOutput.criticalRegressions;
				reportKeyResults.regressions = reportKeyOutput.regressionData.regressionOutput.regressions;
			}
			
			if (reportKeyOutput.transactionDatas != null) {
				Pair<Integer, Integer> slowdownPair = getSlowdowns(reportKeyOutput.transactionDatas);
				reportKeyResults.slowdowns = slowdownPair.getFirst().intValue();
				reportKeyResults.severeSlowdowns = slowdownPair.getSecond().intValue();
			}
			
			if (reportKeyOutput.regressionData != null) {
			
				reportKeyResults.score = getScore(reportSettings, reportKeyOutput.regressionData.regressionOutput, 
						reportKeyResults.slowdowns, reportKeyResults.severeSlowdowns);
				
				result.add(reportKeyResults);

			}			
		}
		
		return result;
	}
	
	private int getStatus(int nonSevere, int severe) {
		
		if (severe > 0) {
			return IssueTypeStatus.RED.ordinal();
		}
		
		if (nonSevere > 4) {
			return IssueTypeStatus.YELLOW.ordinal();
		}
		
		return IssueTypeStatus.GREEN.ordinal();
	}
	
	@Override
	protected List<List<Object>> processServiceEvents(String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan) {

		List<List<Object>> result = new ArrayList<List<Object>>();
		
		Collection<ReportKeyOutput> reportKeyOutputs = executeReport(serviceId, (RelabilityReportInput) input, timeSpan);
		Collection<ReportKeyResults > reportKeyResults = getReportResults(serviceId, reportKeyOutputs);
		
		RelabilityReportInput rrInput = (RelabilityReportInput)input;
		
		for (ReportKeyResults reportKeyResult : reportKeyResults) {
			
			Pair<Object, Object> fromTo = getTimeFilterPair(timeSpan, input.timeFilter);
			String timeRange = TimeUtil.getTimeUnit(input.timeFilter); 
			
			Object newIssuesDesc = formatIssueDesc(rrInput, reportKeyResult.newIssues, reportKeyResult.severeNewIssues);
			Object regressionsDesc = formatIssueDesc(rrInput, reportKeyResult.regressions, reportKeyResult.criticalRegressions);
			Object slowdownsDesc = formatIssueDesc(rrInput, reportKeyResult.slowdowns, reportKeyResult.severeSlowdowns);
				
			Object newIssuesState = getStatus(reportKeyResult.newIssues, reportKeyResult.severeNewIssues);
			Object regressionsState = getStatus(reportKeyResult.regressions, reportKeyResult.criticalRegressions);
			Object slowdownsState = getStatus(reportKeyResult.slowdowns, reportKeyResult.severeSlowdowns);
			
			Object[] row = new Object[] { fromTo.getFirst(), fromTo.getSecond(), timeRange,
					reportKeyResult.output.key, 
					newIssuesDesc, regressionsDesc, slowdownsDesc,
					newIssuesState, regressionsState, slowdownsState,
					reportKeyResult.score};

			result.add(Arrays.asList(row));
		}

		return result;
	}
	
	private String getPostfix(RelabilityReportInput input, double score) {
		
		if (input.thresholds == null) {
			return null;
		}
		
		if (input.postfixes == null) {
			return null;
		}
		
		String[] thresholds = input.thresholds.split(ARRAY_SEPERATOR);
		
		if (thresholds.length != 2) {
			return null;
		}
		
		String[] postfixes = input.postfixes.split(ARRAY_SEPERATOR);
		
		if (postfixes.length != 3) {
			return null;
		}
		
		int min = convert(thresholds[0]);
		int max = convert(thresholds[1]);
		
		if ((min < 0) || (max < 0)) {
			return null;
		}

		if (score < min) {
			return postfixes[0];
		}
		
		if (score < max) {
			return postfixes[1];
		}
		
		return postfixes[2];
	}
	
	private static int convert(String s) {
		try {
			return Integer.parseInt(s);
		} catch (Exception e) {
			return -1;
		}
	}

	private Pair<Integer, Integer> getSlowdowns(Collection<TransactionData> transactionDatas) {
		
		int slowdowns = 0;
		int severeSlowdowns = 0;
		
		for (TransactionData transactionData : transactionDatas) {
			if (PerformanceState.SLOWING.equals(transactionData.state)) {
				slowdowns++;
			} else if (PerformanceState.CRITICAL.equals(transactionData.state)) {
				severeSlowdowns++;
			}
		}
		
		return Pair.of(slowdowns, severeSlowdowns);
	}
	
	private double getGraphValue(RelabilityReportInput input, ReportKeyResults reportKeyResult) {
		
		if (input.graphType == null) {
			return reportKeyResult.score;
		}
		
		GraphType graphType = GraphType.valueOf(input.graphType.replace(" ", ""));
		
		switch (graphType) {
			
			case NewIssues: return reportKeyResult.newIssues + reportKeyResult.severeNewIssues;
			case SevereNewIssues: return reportKeyResult.severeNewIssues; 
			case Regressions: return reportKeyResult.regressions + reportKeyResult.criticalRegressions;
			case SevereRegressions: return reportKeyResult.criticalRegressions; 
			case Slowdowns: return reportKeyResult.slowdowns + reportKeyResult.severeSlowdowns;
			case SevereSlowdowns: return reportKeyResult.severeSlowdowns; 
			case Score: return reportKeyResult.score; 
		}
		
		return 0;
	}
	

	private List<Series> processGraph(RelabilityReportInput input) {

		List<Series> result = new ArrayList<Series>();

		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		Collection<String> serviceIds = getServiceIds(input);

		for (String serviceId : serviceIds) {

			Collection<ReportKeyOutput> reportKeyOutputs = executeReport(serviceId, input, timeSpan);
			Collection<ReportKeyResults > reportKeyResults = getReportResults(serviceId, reportKeyOutputs);

			for (ReportKeyResults reportKeyResult : reportKeyResults) {

				double value = getGraphValue(input, reportKeyResult);
				
				String seriesName;
				String postfix = getPostfix(input, value);
				
				if (postfix != null) {
					seriesName = reportKeyResult.output.key + postfix;
				} else {
					seriesName = reportKeyResult.output.key;
				}
								
				Series series = new Series();
				series.values = new ArrayList<List<Object>>();

				series.name = EMPTY_NAME;
				series.columns = Arrays.asList(new String[] { TIME_COLUMN, seriesName });
				series.values
						.add(Arrays.asList(new Object[] { timeSpan.getFirst().getMillis(), value }));

				result.add(series);
			}
		}

		return result;
	}
}
