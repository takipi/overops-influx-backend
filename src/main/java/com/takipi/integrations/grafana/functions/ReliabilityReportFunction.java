package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.ocpsoft.prettytime.PrettyTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.result.event.EventSlimResult;
import com.takipi.api.client.result.event.EventsSlimVolumeResult;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.performance.calc.PerformanceState;
import com.takipi.api.client.util.regression.RateRegression;
import com.takipi.api.client.util.regression.RegressionResult;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.functions.TransactionsListFunction.TransactionData;
import com.takipi.integrations.grafana.functions.TransactionsListFunction.TransactionDataResult;
import com.takipi.integrations.grafana.input.BaseEventVolumeInput;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.input.RelabilityReportInput;
import com.takipi.integrations.grafana.input.RelabilityReportInput.GraphType;
import com.takipi.integrations.grafana.input.RelabilityReportInput.ReportMode;
import com.takipi.integrations.grafana.input.RelabilityReportInput.ScoreType;
import com.takipi.integrations.grafana.input.RelabilityReportInput.SortType;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings;
import com.takipi.integrations.grafana.settings.input.RegressionReportSettings;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.DeploymentUtil;
import com.takipi.integrations.grafana.util.TimeUtil;

public class ReliabilityReportFunction extends EventsFunction {
	
	private static final Logger logger = LoggerFactory.getLogger(ReliabilityReportFunction.class);
	
	private static final List<String> FIELDS = Arrays.asList(
			new String[] { ViewInput.FROM, ViewInput.TO, ViewInput.TIME_RANGE, 
					"Service", "Key", "Name",
					"NewIssues", "Regressions", "Slowdowns", 
					"NewIssuesDesc", "RegressionsDesc", "SlowdownsDesc", 
					"Score", "ScoreDesc" });
	
	private static final int MAX_ITEMS_DESC = 3; 
	
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
	
protected static class ReportKey implements Comparable<ReportKey>{
		
		protected String name;
		protected boolean isKey;
		
		protected ReportKey(String name, boolean isKey) {
			this.name = name;
			this.isKey = isKey;
		}
		
		@Override
		public String toString()
		{
			return name;
		}
		
		@Override
		public boolean equals(Object obj)
		{
			if (!(obj instanceof ReportKey)) {
				return false;
			}
			
			return Objects.equal(name, ((ReportKey)(obj)).name);
		}
		
		@Override
		public int hashCode()
		{
			return name.hashCode();
		}

		@Override
		public int compareTo(ReportKey o)
		{
			return name.compareTo(o.name);
		}
	}
	
	protected static class ReportKeyOutput {
		protected ReportKey reportKey;
		protected RegressionData regressionData;
		protected Collection<TransactionData> transactionDatas;
		
		protected ReportKeyOutput(ReportKey key) {
			this.reportKey = key;
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
		protected String slowDownsDesc; 
		protected String regressionsDesc; 
		protected String newIssuesDesc; 
		protected String description; 
		protected double score;
		protected String scoreDesc;
		protected long volume;
		protected int uniqueEvents;
		
		protected ReportKeyResults(ReportKeyOutput output) {
			this.output = output;	
		}
	}
	
	protected static class ReportAsyncResult {
		protected ReportKey key;
		
		protected ReportAsyncResult(ReportKey key) {
			this.key = key;
		}
	}
	
	protected static class RegressionAsyncResult extends ReportAsyncResult {
		protected RegressionOutput output;
		
		protected RegressionAsyncResult(ReportKey key, RegressionOutput output) {
			super(key);
			this.output = output;
		}
	}
	
	protected static class SlowdownAsyncResult extends ReportAsyncResult{
		
		protected Collection<TransactionData> transactionDatas;
		
		protected SlowdownAsyncResult(ReportKey key, Collection<TransactionData> transactionDatas) {
			super(key);
			this.transactionDatas = transactionDatas;
		}
	}

	public static class SlowdownAsyncTask extends BaseAsyncTask implements Callable<Object> {
		protected TransactionsListFunction function;
		protected ReportKey reportKey;
		protected String serviceId;
		protected BaseEventVolumeInput input;
		protected Pair<DateTime, DateTime> timeSpan;

		protected SlowdownAsyncTask(TransactionsListFunction function, ReportKey key, String serviceId, RegressionsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			this.function = function;
			this.reportKey = key;
			this.serviceId = serviceId;
			this.input = input;
			this.timeSpan = timeSpan;
		}

		@Override
		public Object call() {

			beforeCall();

			try {

				TransactionDataResult transactionDataResult = function.getTransactionDatas(serviceId, timeSpan, input, false, 0);				
				
				SlowdownAsyncResult result;
				
				if (transactionDataResult != null) {
					result = new SlowdownAsyncResult(reportKey, transactionDataResult.items.values());
				} else {
					result = new SlowdownAsyncResult(reportKey, Collections.emptyList());
				}
							
				return result;
			} finally {
				afterCall();
			}
		}

		@Override
		public String toString() {
			return String.join(" ", "Regression", serviceId, reportKey.name);
		}
	}
	
	public static class RegressionAsyncTask extends BaseAsyncTask implements Callable<Object> {

		protected RegressionFunction function;
		protected ReportKey reportKey;
		protected String serviceId;
		protected RegressionsInput input;
		protected Pair<DateTime, DateTime> timeSpan;

		protected RegressionAsyncTask(RegressionFunction function, ReportKey key, String serviceId, RegressionsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			this.function = function;
			this.reportKey = key;
			this.serviceId = serviceId;
			this.input = input;
			this.timeSpan = timeSpan;
		}

		@Override
		public Object call() {

			beforeCall();

			try {

				RegressionOutput output = function.runRegression(serviceId, input); 
				RegressionAsyncResult result = new RegressionAsyncResult(reportKey, output);
				
				return result;
			} finally {
				afterCall();
			}

		}

		@Override
		public String toString() {
			return String.join(" ", "Regression", serviceId, reportKey.name);
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

	private RelabilityReportInput getInput(RelabilityReportInput reportInput, 
		String name, boolean mustCopy) {
		
		ReportMode mode = reportInput.getReportMode();
		
		if ((mode == ReportMode.Default) && (!mustCopy)) {
			return reportInput;
		}
		
		Gson gson = new Gson();
		String json = gson.toJson(reportInput);
		RelabilityReportInput result = gson.fromJson(json, RelabilityReportInput.class);

		if (mode == ReportMode.Default) {
			return result;
		}
		
		switch (mode) {
		
			case Deployments:
				result.deployments = name;
				break;
				
			case Tiers:
				result.types = EventFilter.CATEGORY_PREFIX + name;
				break;
			
			case Applications:
				result.applications = name;
				break;
			
			default:
				break;	
		}

		return result;
	}

	@Override
	protected List<String> getColumns(String fields) {
		return FIELDS;
	}

	private Collection<ReportKey> getTiers(String serviceId, RelabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan) {
 		
		Collection<String> keyTiers = GrafanaSettings.getServiceSettings(apiClient, serviceId).getTierNames();
		
		Collection<String> types = input.getTypes();
		
		Set<ReportKey> result = new TreeSet<ReportKey>();
		
		if (!CollectionUtil.safeIsEmpty(types)) {
						
			for (String type : types) {
				if (type.startsWith(EventFilter.CATEGORY_PREFIX)) {
					String name = type.substring(EventFilter.CATEGORY_PREFIX.length());
					boolean isKey = CollectionUtil.safeContains(keyTiers, name);
					ReportKey key = new ReportKey(name, isKey);
					result.add(key);
				}
			}
			
			if (result.size() > 0) {
				return result;
			}
		}
			
		if (keyTiers != null) {
			for (String keyTier: keyTiers) {
				result.add(new ReportKey(keyTier, true));
			}
		} 
		
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timeSpan.getFirst(), 
			timeSpan.getSecond(), null);
		
		if (eventsMap == null) {
			return Collections.emptyList();
		}
		
		Categories categories = GrafanaSettings.getServiceSettings(apiClient, serviceId).getCategories();
				
		for (EventResult event : eventsMap.values()) {
						
			if (event.error_origin != null) {
				
				Set<String> originlabels = categories.getCategories(event.error_origin.class_name);
				
				if (originlabels != null) {
					result.addAll(toReportKeys(originlabels, false));
				}
			}
			
			if (event.error_location != null) {
				
				Set<String> locationLabels = categories.getCategories(event.error_location.class_name);
				
				if (locationLabels != null) {
					result.addAll(toReportKeys(locationLabels, false));
				}
			}
		}
		
		return result;
	}

	private List<VolumeOutput> getAppVolumes(String serviceId, RelabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan, Collection<String> apps) {

		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();

		for (String app : apps) {
			RegressionsInput appInput = getInput(input, app, false);
			tasks.add(new AppVolumeAsyncTask(appInput, serviceId, app, timeSpan));
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

		protected ReportKey reportKey;
		protected RateRegression regression;
		protected RegressionOutput regressionOutput;

		protected RegressionData(RateRegression regression, ReportKey key,
				RegressionOutput regressionOutput) {

			this.reportKey = key;
			this.regression = regression;
			this.regressionOutput = regressionOutput;
		}
	}

	protected List<ReportAsyncResult> processAsync(String serviceId, RelabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan, Collection<ReportKey> reportKeys) {

		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
		List<ReportAsyncResult> result = new ArrayList<ReportAsyncResult>();
				
		ScoreType scoreType = input.getScoreType();
		
		for (ReportKey reportKey : reportKeys) {
			
			RegressionFunction regressionFunction = new RegressionFunction(apiClient);
			TransactionsListFunction transactionsListFunction = new TransactionsListFunction(apiClient);

			RegressionsInput regressionsInput = getInput(input, reportKey.name, false);
			RegressionsInput transactionInput = getInput(input, reportKey.name, true);

			transactionInput.pointsWanted = input.transactionPointsWanted;
			
			if ((scoreType == ScoreType.Combined) || (scoreType == ScoreType.Regressions)) {

				RegressionOutput regressionOutput = ApiCache.getRegressionOutput(apiClient, serviceId, regressionsInput, regressionFunction, false);
						
				if (regressionOutput != null) {
					result.add(new RegressionAsyncResult(reportKey, regressionOutput));
				} else {
					tasks.add(new RegressionAsyncTask(regressionFunction, reportKey, serviceId, regressionsInput, timeSpan));
				}
			}
			
			if ((scoreType == ScoreType.Combined) || (scoreType == ScoreType.Slowdowns)) {
				tasks.add(new SlowdownAsyncTask(transactionsListFunction, 
					reportKey, serviceId, transactionInput, timeSpan));
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

	private Collection<ReportKey> getActiveApplications(String serviceId, RelabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan) {

		List<String> keyApps = new ArrayList<String>();
		
		GroupSettings appGroups = GrafanaSettings.getData(apiClient, serviceId).applications;
		
		if (appGroups != null) {
			keyApps.addAll(appGroups.getAllGroupValues());
		}
		
		List<ReportKey> result;
		Collection<String> selectedApps = input.getApplications(apiClient, serviceId);
		
		if (!CollectionUtil.safeIsEmpty(selectedApps)) {
			
			result = new ArrayList<ReportKey>();
			
			for (String selectedApp : selectedApps) {
				boolean isKey = keyApps.contains(selectedApp);
				result.add(new ReportKey(selectedApp, isKey));
			}
			
			return result;
		}
			
		if (keyApps.size() > input.limit) {
			return toReportKeys(keyApps, true);
		}
		
		Collection<String> apps = ClientUtil.getApplications(apiClient, serviceId, true);
		
		if (keyApps.size() + apps.size() < input.limit) {
			result = new ArrayList<ReportKey>(keyApps.size() + apps.size());
			result.addAll(toReportKeys(keyApps, true));
			result.addAll(toReportKeys(apps, false));
			return result;
		}
		
		List<VolumeOutput> appVolumes = getAppVolumes(serviceId, input, timeSpan, apps);
		Collection<String> appsByVolume = limitVolumes(appVolumes, input.limit);
		
		result = new ArrayList<ReportKey>(toReportKeys(appsByVolume, false));
		
		if (result.size() > input.limit) {
			return result.subList(0, input.limit);
		}
		
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

	private Collection<ReportKey> toReportKeys(Collection<String> keys, boolean isKey) {
		
		List<ReportKey> result = new ArrayList<ReportKey>(keys.size());
		
		for (String key : keys) {
			result.add(new ReportKey(key, isKey));
		}
		
		return result;
	}
	
	private Collection<ReportKey> getActiveDeployments(String serviceId, RelabilityReportInput input) {

		List<String> selectedDeployments = input.getDeployments(serviceId);
		
		if (!CollectionUtil.safeIsEmpty(selectedDeployments)) {
			return toReportKeys(selectedDeployments, false);
		}
		
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

		return toReportKeys(result, false);
	}
	
	private Collection<ReportKey> getActiveKeys(String serviceId, RelabilityReportInput regInput,
			Pair<DateTime, DateTime> timeSpan) {
		
		Collection<ReportKey> keys;
			
		ReportMode mode = regInput.getReportMode();
		
		switch (mode) {
				
			case Deployments:
				keys = getActiveDeployments(serviceId, regInput);
				break;
					
			case Tiers: 
				keys = getTiers(serviceId, regInput, timeSpan);
				break;
				
			case Applications: 
				keys = getActiveApplications(serviceId, regInput, timeSpan);
				break;
					
			case Default: 
				keys = Collections.singleton(new ReportKey("", false));
				break;
					
			default: 
				throw new IllegalStateException("Unsopported mode " + mode);
			}

		return keys;
	}
	
	private static void addDeduction(String name, int value, int weight, List<String> deductions) {
		if (value == 0) {
			return;
		}
		
		StringBuilder builder = new StringBuilder();
		
		if (value > 0) {
			builder.append(value);
			builder.append(" ");
			builder.append(name);
			
			if (value > 1) {
				builder.append("s");
			}
			
			if (weight > 1) {
				builder.append(" * ");
				builder.append(weight);
			}
		}
		
		deductions.add(builder.toString());
	}
	
	private static int getRegressionScoreWindow(RegressionOutput regressionOutput) {
		
		int result = 0;
		
		if (CollectionUtil.safeIsEmpty(regressionOutput.regressionInput.deployments)) {
			result = regressionOutput.regressionInput.activeTimespan;
		} else {
			DateTime lastPointTime = null;
			
			for (int i = regressionOutput.activeVolumeGraph.points.size() - 1; i >= 0; i--) {
				
				GraphPoint gp = regressionOutput.activeVolumeGraph.points.get(i);
				
				if ((gp.stats != null) && ((gp.stats.invocations > 0) || (gp.stats.hits > 0))) {
					lastPointTime = TimeUtil.getDateTime(gp.time);
					break;
				}
			}
			
			if (lastPointTime != null) {
				long delta  = lastPointTime.minus(regressionOutput.regressionInput.activeWindowStart.getMillis()).getMillis();
				result = (int)TimeUnit.MILLISECONDS.toMinutes(delta);
			} else {
				result = regressionOutput.regressionInput.activeTimespan;			
			}			
		}
		
		return result;
	}
	
	public static Pair<Double, String> getScore(RegressionReportSettings reportSettings, 
		ReportKeyResults reportKeyResults) {
		
		RegressionOutput regressionOutput = reportKeyResults.output.regressionData.regressionOutput;
		
		int newEventsScore = regressionOutput.newIssues * reportSettings.new_event_score;
		int severeNewEventScore = regressionOutput.severeNewIssues * reportSettings.severe_new_event_score;
		int criticalRegressionsScore = (regressionOutput.criticalRegressions + reportKeyResults.severeSlowdowns) * reportSettings.critical_regression_score;
		int regressionsScore = (regressionOutput.regressions + reportKeyResults.slowdowns) * reportSettings.regression_score;
		
		int scoreWindow = getRegressionScoreWindow(regressionOutput);		
		double scoreDays = Math.max(1, (double)scoreWindow / 60 / 24);
	
		double weight;
		
		if (reportKeyResults.output.reportKey.isKey) {
			weight = reportSettings.key_score_weight;
		} else {
			weight = reportSettings.score_weight;	
		}
		
		double rawScore = (newEventsScore + severeNewEventScore + criticalRegressionsScore + regressionsScore) / scoreDays;
		double resultScore = Math.max(100 - (weight * rawScore), 0);
		
		String description = getScoreDescription(reportSettings, reportKeyResults,
				resultScore, scoreWindow);
		
		return Pair.of(resultScore, description);
	}
	
	private static String getScoreDescription(RegressionReportSettings reportSettings,
		ReportKeyResults reportKeyResults, double resultScore, int period) {
		
		StringBuilder result = new StringBuilder();

		PrettyTime prettyTime = new PrettyTime();
		String duration = prettyTime.formatDuration(new DateTime().minusMinutes(period).toDate());
		
		RegressionOutput regressionOutput = reportKeyResults.output.regressionData.regressionOutput;

		result.append("100");
		
		int allIssuesCount = regressionOutput.newIssues + regressionOutput.severeNewIssues + 
				regressionOutput.criticalRegressions + regressionOutput.regressions +
				reportKeyResults.slowdowns + reportKeyResults.severeSlowdowns;
		
		if (allIssuesCount > 0) {
			result.append(" - (");
			
			List<String> deductions = new ArrayList<String>();
			
			addDeduction("new issue", regressionOutput.newIssues, reportSettings.new_event_score, deductions);
			addDeduction("severe new issue", regressionOutput.severeNewIssues, reportSettings.severe_new_event_score, deductions);
			addDeduction("error increase", regressionOutput.regressions, reportSettings.regression_score, deductions);
			addDeduction("severe error increase", regressionOutput.criticalRegressions, reportSettings.critical_regression_score, deductions);
			addDeduction("slowdown", reportKeyResults.slowdowns, reportSettings.regression_score, deductions);
			addDeduction("severe slowdown", reportKeyResults.severeSlowdowns, reportSettings.critical_regression_score, deductions);
	
			String deductionString = String.join(" + ", deductions);
			result.append(deductionString);
			result.append(") * ");
			
			if (reportKeyResults.output.reportKey.isKey) {
				result.append(reportSettings.key_score_weight);
			} else {
				result.append(reportSettings.score_weight);
			}
			
			result.append(", avg over ");
			result.append(duration);
			result.append(" = ");
			result.append(decimalFormat.format(resultScore));
		}
		
		return result.toString(); 
	}
	
	protected Collection<ReportKeyOutput> executeReport(String serviceId, RelabilityReportInput regInput,
			Pair<DateTime, DateTime> timeSpan) {
		
		Collection<ReportKey> keys = getActiveKeys(serviceId, regInput, timeSpan);
		
		logger.debug("Executing report " + regInput.getReportMode() + " keys: " + Arrays.toString(keys.toArray()));

		List<ReportAsyncResult> AsyncResults = processAsync(serviceId, regInput, timeSpan, keys);

		Map<ReportKey, ReportKeyOutput> reportKeyOutputMap = new HashMap<ReportKey, ReportKeyOutput>();
		
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
				
				SlowdownAsyncResult slowdownAsyncResult = (SlowdownAsyncResult)asyncResult;
				reportKeyOutput.transactionDatas = slowdownAsyncResult.transactionDatas;
			}
			
		}
		
		List<ReportKeyOutput> result;
		
		if (regInput.getReportMode() == ReportMode.Deployments) {
			boolean sortAsc = getSortedAsc(regInput.getSortType(), true);
			List<ReportKeyOutput> sorted = new ArrayList<ReportKeyOutput>(reportKeyOutputMap.values());
			sortDeployments(sorted, sortAsc);
			result = sorted;
		} else {
			
			boolean sortAsc = getSortedAsc(regInput.getSortType(), true);
			List<ReportKeyOutput> sorted = new ArrayList<ReportKeyOutput>(reportKeyOutputMap.values());
			sortKeys(sorted, sortAsc);
			result = sorted;			
		}
		
		return result;
	}
	
	private boolean getSortedAsc(SortType sortType, boolean defaultValue) {
		
		switch (sortType) {
			case Ascending: 
				return true;
				
			case Descending: 
				return false;
				
			case Default: 
				return defaultValue;
				
			default:
				throw new IllegalStateException();
		}
	}
	
	private void sortDeployments(List<ReportKeyOutput> scores, boolean asc) {
		scores.sort(new Comparator<ReportKeyOutput>() {

			@Override
			public int compare(ReportKeyOutput o1, ReportKeyOutput o2) {

				if (asc) {
					return DeploymentUtil.compareDeployments(o1.reportKey, o2.reportKey);
				} else {
					return DeploymentUtil.compareDeployments(o2.reportKey, o1.reportKey);
				}
			}
		});
	}
	
	private void sortKeys(List<ReportKeyOutput> scores, boolean asc) {
		
		scores.sort(new Comparator<ReportKeyOutput>() {

			@Override
			public int compare(ReportKeyOutput o1, ReportKeyOutput o2) {

				if (asc) {
					return o1.reportKey.name.compareTo(o2.reportKey.name);
				} else {
					return o2.reportKey.name.compareTo(o1.reportKey.name);
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
		Collection<ReportKeyResults> reportKeyResults = getReportResults(serviceId, input, reportKeyOutputs);
		
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
		if (CollectionUtil.safeIsEmpty(serviceIds)) {
			return 0;
		}
		
		double sum = 0;
		
		for (String serviceId : serviceIds)
		{
			sum += getServiceSingleStat(serviceId, timeSpan, input);
		}
		
		double result = sum / serviceIds.size();
		
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

	private static Object formatValue(RelabilityReportInput input, int nonSevere, int severe)
	{
		
		Object result;
		
		if (severe > 0)
		{
			if (nonSevere == 0)
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
			if (nonSevere != 0) {
				result = Integer.valueOf(nonSevere);
			} else {
				result = "";
			}
		}
		
		return result;
	}

	private String getNewIssuesDesc(RateRegression rateRegression, int newIssues,
			int severeNewIssues) {
		
		StringBuilder result = new StringBuilder();
		
		int size = 0;
		
		Collection<EventResult> newEvents =  rateRegression.getSortedAllNewEvents();
		
		for (EventResult newEvent : newEvents) {
			
			if (newEvent.stats.hits > 0) {
				result.append(newEvent.name);
				result.append(" in ");
				result.append(getSimpleClassName(newEvent.error_location.class_name));
				
				size++;
			} else {
				continue;
			}
				
			if (size == MAX_ITEMS_DESC) {
				break;
			} else {
				if (size < newIssues + severeNewIssues) {
					result.append(", ");
				}
			}
		}
		
		int remaining = newIssues + severeNewIssues - size;
		
		if (remaining > 0) {
			result.append("\nand ");
			result.append(remaining);
			result.append(" more");
		}
		
		return result.toString();
	}
	
	private String getRegressionsDesc(RateRegression rateRegression, int regressionsSize,
		int severeRegressionsSize) {
		
		StringBuilder result = new StringBuilder();
		
		int size = 0;
		
		Collection<RegressionResult> regressions =  rateRegression.getSortedAllRegressions();
		
		for (RegressionResult regressionResult : regressions) {
			
			double baseRate = (double) regressionResult.getBaselineHits() / (double) regressionResult.getBaselineInvocations();
			double activeRate = (double) regressionResult.getEvent().stats.hits / (double) regressionResult.getEvent().stats.invocations;

			int delta = (int)((activeRate - baseRate) * 100);
			
			if (delta < 1000) {
				result.append("+"); 
				result.append(delta);
			} else {
				result.append(">1000"); 
			}
			
			result.append("% "); 

			result.append(regressionResult.getEvent().name);
			result.append(" in ");
			result.append(getSimpleClassName(regressionResult.getEvent().error_location.class_name));
						
			size++;
			
			if (size == MAX_ITEMS_DESC) {
				break;
			} else {
				if (size < regressionsSize + severeRegressionsSize) {
					result.append(", ");
				}
			}
		}
		
		int remaining = regressionsSize + severeRegressionsSize - size;
		
		if (remaining > 0) {
			result.append("\nand ");
			result.append(remaining);
			result.append(" more");
		}
		
		return result.toString();
	}
	
	private String getSlowdownRate(TransactionData transactionData) {
		return Math.round(transactionData.stats.avg_time / transactionData.baselineAvg * 100) + "%";
	}
	
	private String getSlowdownsDesc(Collection<TransactionData> transactionDatas, 
		int slowdownsSize, int severeSlowdownsSize) {
		
		StringBuilder result = new StringBuilder();
					
		List<TransactionData> slowdowns = new ArrayList<TransactionData>();
		List<TransactionData> severeSlowdowns = new ArrayList<TransactionData>();
		
		for (TransactionData transactionData : transactionDatas) {
			
			if (transactionData.state == PerformanceState.CRITICAL) {
				severeSlowdowns.add(transactionData);
			} else if (transactionData.state == PerformanceState.SLOWING) {
				slowdowns.add(transactionData);	
			}
			
			if (severeSlowdowns.size() + slowdowns.size() >= MAX_ITEMS_DESC) {
				break;
			}
		}
		
		int index = 0;
		
		for (TransactionData transactionData : severeSlowdowns) {
		
			result.append("+");
			result.append(getSlowdownRate(transactionData));
			result.append(" ");
			result.append(getTransactionName(transactionData.graph.name, false));
						
			index++;
			
			if ((index < severeSlowdowns.size()) || (slowdowns.size() > 0)) {
				result.append(", ");
			}
		}
		
		index = 0;
		
		for (TransactionData transactionData : slowdowns) {
			
			result.append("+");
			result.append(getSlowdownRate(transactionData));
			result.append(" ");
			result.append(getTransactionName(transactionData.graph.name, false));
		
			index++;
		
			if (index < slowdowns.size()) {
				result.append(", ");
			}
		}
		
		int remaining = slowdowns.size() + severeSlowdowns.size() - severeSlowdownsSize -  slowdownsSize;
		
		if (remaining > 0) {
			result.append(" and ");
			result.append(remaining);
			result.append(" more");
		}
		
		return result.toString();
	}
	
	private Collection<ReportKeyResults> getReportResults(String serviceId, 
			RelabilityReportInput input, Collection<ReportKeyOutput> reportKeyOutputs) {
		
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
				
				reportKeyResults.newIssuesDesc = getNewIssuesDesc(reportKeyOutput.regressionData.regressionOutput.rateRegression,
					reportKeyResults.newIssues, reportKeyResults.severeNewIssues);
					
				reportKeyResults.regressionsDesc = getRegressionsDesc(reportKeyOutput.regressionData.regressionOutput.rateRegression,
						reportKeyResults.regressions, reportKeyResults.criticalRegressions);

			}
			
			if (reportKeyOutput.transactionDatas != null) {
				
				Pair<Integer, Integer> slowdownPair = getSlowdowns(reportKeyOutput.transactionDatas);
				reportKeyResults.slowdowns = slowdownPair.getFirst().intValue();
				reportKeyResults.severeSlowdowns = slowdownPair.getSecond().intValue();
				reportKeyResults.slowDownsDesc = getSlowdownsDesc(reportKeyOutput.transactionDatas,
						reportKeyResults.slowdowns, reportKeyResults.severeSlowdowns);
			}
			
			if (reportKeyOutput.regressionData != null) {
			
				Pair<Double, String> scorePair = getScore(reportSettings, reportKeyResults);
				
				reportKeyResults.score = scorePair.getFirst();
				reportKeyResults.scoreDesc = scorePair.getSecond();
				
				reportKeyResults.description = getDescription(reportKeyOutput.regressionData, 
					reportKeyResults.newIssuesDesc, reportKeyResults.regressionsDesc, reportKeyResults.slowDownsDesc);
					
				 Pair<Long, Integer>  volumePair = getKeyOutputEventVolume(reportKeyOutput);
				 
				 if (volumePair != null) {
					 reportKeyResults.volume = volumePair.getFirst().longValue();
					 reportKeyResults.uniqueEvents = volumePair.getSecond().intValue();
				 }
				
				result.add(reportKeyResults);
			}			
		}
		
		if (input.limit < result.size()) {
			return limitByScore(result, input.limit);
		}
		
		return result;
	}
	
	private Collection<ReportKeyResults> limitByScore(List<ReportKeyResults> results, int limit) {
		
		results.sort(new Comparator<ReportKeyResults>()
		{
			@Override
			public int compare(ReportKeyResults o1, ReportKeyResults o2)
			{	
				ReportKey k1 = o1.output.reportKey;
				ReportKey k2 = o2.output.reportKey;
				
				int scoreDelta = (int)Math.round((o1.score - o2.score) * 100);

				if (k2.isKey) {
					if (k1.isKey) {
						return scoreDelta;
					} else {
						return 1;
					}
				} else if (k1.isKey) {
					return -1;
				} else {
					return scoreDelta;
				}
			}
		});
		
		return results.subList(0,  limit);
	}
	
	private Pair<Long, Integer> getKeyOutputEventVolume(ReportKeyOutput output) {
		
		Collection<EventResult> events = output.regressionData.regressionOutput.regressionInput.events;
		
		if (events == null) {
			return null;
		}
		
		int count = 0;
		long volume = 0l;
		
		for (EventResult event : events) {
			
			if (event.stats != null) {
				volume += event.stats.hits;
				count++;
			}	
		}
		
		return Pair.of(volume, count);
	}
	
	private String getDescription(RegressionData regressionData, String newErrorsDesc, 
		String regressionDesc, String slowdownDesc) {
		
		StringBuilder result = new StringBuilder();
		
		result.append(regressionData.reportKey);
		result.append(" over ");
		
		DateTime activeWindow = regressionData.regressionOutput.regressionInput.activeWindowStart;
		result.append(". ");
		
		result.append(new PrettyTime().format(new Date(activeWindow.getMillis())));
		
		if ((newErrorsDesc != null) && (newErrorsDesc.length() > 0)) {
			result.append("New errors: ");
			result.append(newErrorsDesc);
		}
		

		if ((regressionDesc != null) && (regressionDesc.length() > 0)) {
			result.append("Increasing Errors: ");
			result.append(regressionDesc);
		}
		
		if ((slowdownDesc != null) && (slowdownDesc.length() > 0)) {
			result.append("Slowdowns: ");
			result.append(slowdownDesc);
		}
		
		return result.toString();
		
	}
	
	@Override
	protected List<List<Object>> processServiceEvents(Collection<String> serviceIds,
			String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan) {

		List<List<Object>> result = new ArrayList<List<Object>>();
		
		RelabilityReportInput rrInput = (RelabilityReportInput)input;
		
		Collection<ReportKeyOutput> reportKeyOutputs = executeReport(serviceId, rrInput, timeSpan);
		Collection<ReportKeyResults > reportKeyResults = getReportResults(serviceId, rrInput, reportKeyOutputs);
				
		for (ReportKeyResults reportKeyResult : reportKeyResults) {
			
			RegressionWindow regressionWindow = reportKeyResult.output.regressionData.regressionOutput.regressionWindow;
			
			String timeRange;
			Pair<Object, Object> fromTo;
			
			if (regressionWindow != null) {
				timeRange = TimeUtil.getTimeInterval(TimeUnit.MINUTES.toMillis(regressionWindow.activeTimespan));
				DateTime to = regressionWindow.activeWindowStart.plusMinutes(regressionWindow.activeTimespan);
				fromTo = Pair.of(regressionWindow.activeWindowStart, to);
			} else {
				fromTo = getTimeFilterPair(timeSpan, input.timeFilter);
				timeRange = TimeUtil.getTimeRange(input.timeFilter);	
			}
			
			
			Object newIssuesValue = formatValue(rrInput, reportKeyResult.newIssues, reportKeyResult.severeNewIssues);
			Object regressionsValue = formatValue(rrInput, reportKeyResult.regressions, reportKeyResult.criticalRegressions);
			Object slowdownsValue = formatValue(rrInput, reportKeyResult.slowdowns, reportKeyResult.severeSlowdowns);
					
			Object[] row = new Object[] { fromTo.getFirst(), fromTo.getSecond(), timeRange,
					serviceId, reportKeyResult.output.reportKey.name,
					getServiceValue(getKeyName(reportKeyResult), serviceId, serviceIds),
					newIssuesValue, regressionsValue, slowdownsValue,
					reportKeyResult.newIssuesDesc, reportKeyResult.regressionsDesc, reportKeyResult.slowDownsDesc,
					reportKeyResult.score,
					reportKeyResult.scoreDesc,
					reportKeyResult.description};

			result.add(Arrays.asList(row));
		}

		return result;
	}
	
	private String getKeyName(ReportKeyResults reportKeyResult) {
		
		if (reportKeyResult.output.reportKey.isKey) {
			return reportKeyResult.output.reportKey.name  + "*";
		} else {
			return reportKeyResult.output.reportKey.name;
		}
	}
	
	private String getPostfix(RelabilityReportInput input, double score) {
		
		GraphType graphType = getGraphType(input);
		
		if (graphType != GraphType.Score) {
			return null;
		}
		
		if (input.thresholds == null) {
			return null;
		}
		
		if (input.postfixes == null) {
			return null;
		}
		
		String[] thresholds = input.thresholds.split(GRAFANA_SEPERATOR);
		
		if (thresholds.length != 2) {
			return null;
		}
		
		String[] postfixes = input.postfixes.split(GRAFANA_SEPERATOR);
		
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
	
	private GraphType getGraphType(RelabilityReportInput input) {
		
		if (input.graphType == null) {
			return null;
		}
		
		GraphType result = GraphType.valueOf(input.graphType.replace(" ", ""));
		return result;
	}
	
	private double getGraphValue(RelabilityReportInput input, ReportKeyResults reportKeyResult) {
		
		GraphType graphType = getGraphType(input);

		if (graphType == null) {
			return reportKeyResult.score;
		}
		
		switch (graphType) {
			
			case NewIssues: return reportKeyResult.newIssues + reportKeyResult.severeNewIssues;
			case SevereNewIssues: return reportKeyResult.severeNewIssues; 
			case Regressions: return reportKeyResult.regressions + reportKeyResult.criticalRegressions;
			case SevereRegressions: return reportKeyResult.criticalRegressions; 
			case Slowdowns: return reportKeyResult.slowdowns + reportKeyResult.severeSlowdowns;
			case SevereSlowdowns: return reportKeyResult.severeSlowdowns; 
			case EventVolume: return reportKeyResult.volume; 
			case UniqueEvents: return reportKeyResult.uniqueEvents;
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
			Collection<ReportKeyResults > reportKeyResults = getReportResults(serviceId, input, reportKeyOutputs);

			for (ReportKeyResults reportKeyResult : reportKeyResults) {

				double value = getGraphValue(input, reportKeyResult);
				
				String seriesName;
				String postfix = getPostfix(input, value);
				
				String name = getKeyName(reportKeyResult);
				
				if (postfix != null) {
					seriesName = getServiceValue(name + postfix, serviceId, serviceIds);
				} else {
					seriesName = getServiceValue(name, serviceId, serviceIds);
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
