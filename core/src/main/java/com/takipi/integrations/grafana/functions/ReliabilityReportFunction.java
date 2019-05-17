package com.takipi.integrations.grafana.functions;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.deployment.SummarizedDeployment;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.data.service.SummarizedService;
import com.takipi.api.client.data.settings.AlertSettings;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.data.view.SummarizedView;
import com.takipi.api.client.data.view.ViewInfo;
import com.takipi.api.client.result.category.CategoriesResult;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.result.event.EventSlimResult;
import com.takipi.api.client.result.event.EventsSlimVolumeResult;
import com.takipi.api.client.result.service.ServicesResult;
import com.takipi.api.client.result.settings.AlertsSettingsResult;
import com.takipi.api.client.result.view.ViewsResult;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.infra.Categories.Category;
import com.takipi.api.client.util.infra.Categories.CategoryType;
import com.takipi.api.client.util.infra.InfraUtil;
import com.takipi.api.client.util.performance.calc.PerformanceState;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.api.client.util.settings.GroupSettings;
import com.takipi.api.client.util.settings.RegressionReportSettings;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionOutput;
import com.takipi.integrations.grafana.functions.ReliabilityKpiGraphFunction.KpiInterval;
import com.takipi.integrations.grafana.functions.ReliabilityKpiGraphFunction.ScoreInterval;
import com.takipi.integrations.grafana.functions.ReliabilityKpiGraphFunction.TaskKpiResult;
import com.takipi.integrations.grafana.input.EnvironmentsFilterInput;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.input.RegressionsInput.RenderMode;
import com.takipi.integrations.grafana.input.ReliabilityKpiGraphInput;
import com.takipi.integrations.grafana.input.ReliabilityReportInput;
import com.takipi.integrations.grafana.input.ReliabilityReportInput.ReliabilityKpi;
import com.takipi.integrations.grafana.input.ReliabilityReportInput.ReliabilityState;
import com.takipi.integrations.grafana.input.ReliabilityReportInput.ReportMode;
import com.takipi.integrations.grafana.input.ReliabilityReportInput.ScoreType;
import com.takipi.integrations.grafana.input.ReliabilityReportInput.SortType;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.DeploymentUtil;
import com.takipi.integrations.grafana.util.TimeUtil;

public class ReliabilityReportFunction extends EventsFunction {
	
	private static final Logger logger = LoggerFactory.getLogger(ReliabilityReportFunction.class);
	
	private static final String DEFAULT_KEY = "";
	private static final String KEY_POSTFIX = "*";

	private static final SimpleDateFormat singleDayDateformat;
	private static final SimpleDateFormat dayInMonthDateformat;
		
	private static final int MAX_FIND_DEP_TRIES = 5;
	
	static  {
		singleDayDateformat = new SimpleDateFormat("EEEE"); 
		dayInMonthDateformat = new SimpleDateFormat("EEE, MMM d"); 
		
		singleDayDateformat.setTimeZone(TimeZone.getTimeZone("UTC"));
		dayInMonthDateformat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	public static class Factory implements FunctionFactory {
		
		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ReliabilityReportFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass() {
			return ReliabilityReportInput.class;
		}
		
		@Override
		public String getName() {
			return "regressionReport";
		}
	}
	
	protected class ReportKey implements Comparable<ReportKey>{
		
		protected String name;
		protected boolean isKey;
		
		protected ReportKey(String name, boolean isKey) {
			this.name = name;
			this.isKey = isKey;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof ReportKey)) {
				return false;
			}
			
			return Objects.equal(name, ((ReportKey)(obj)).name);
		}
		
		@Override
		public int hashCode() {
			return name.hashCode();
		}

		@Override
		public int compareTo(ReportKey o) {
			return name.compareTo(o.name);
		}
		
		@Override
		public String toString() {
			
			String result;
			String keyName = GroupSettings.fromGroupName(name);
			
			if (isKey) {
				result =  keyName  + KEY_POSTFIX;
			} else {
				result = keyName;
			}
			
			return result;
		}
		
		/**
		 * @param serviceId - needed for children 
		 */
		public int getWeight(String serviceId) {
			return 1;
		}
		
		/**
		 * @param serviceId - needed for children 
		 * @param reportKeyResults - needed for children 
		 */
		public String getFullName(String serviceId, ReportKeyResults reportKeyResults) {
			return name;
		}
	}
	
	protected class DeploymentReportKey extends ReportKey {
		
		protected SummarizedDeployment current;
		protected SummarizedDeployment compareToDep;

		protected DeploymentReportKey(String name, boolean isKey, 
			SummarizedDeployment current) {
			super(name, isKey);
			
			this.current = current;
		}
		
		@Override
		public String getFullName(String serviceId, ReportKeyResults reportKeyResults) {
			
			StringBuilder result = new StringBuilder();
			
			result.append(name);
			result.append(", introduced ");
			
			int activeTimespan = reportKeyResults.output.regressionData.regressionOutput.regressionInput.activeTimespan;
			Date introduced = DateTime.now().minusMinutes(activeTimespan).toDate();
			result.append(prettyTime.format(introduced));
								
			if ((current != null) && (current.first_seen != null) && (current.last_seen != null)) {
							
				DateTime firstSeen = TimeUtil.getDateTime(current.first_seen);
				DateTime lastSeen = TimeUtil.getDateTime(current.last_seen);

				long delta = lastSeen.getMillis() - firstSeen.getMillis();
				Date lifetime = DateTime.now().minus(delta).toDate();
					
				result.append(", lifetime: ");
				result.append(prettyTime.formatDuration(lifetime));					
			}
			
			return result.toString();
		}
	}
	
	protected class AppReportKey extends ReportKey {

		protected AppReportKey(String name, boolean isKey) {
			super(name, isKey);
		}
		
		@Override
		public int getWeight(String serviceId) {
			
			int result;
			
			Collection<String> apps = EnvironmentsFilterInput.getApplications(apiClient,
				getSettingsData(serviceId), serviceId, name, true, true);
				
			if (!CollectionUtil.safeIsEmpty(apps)) {
				result = apps.size();
			} else {
				result = 0;
			}
			
			
			return result;
		}
		
		@Override
		public String getFullName(String serviceId, ReportKeyResults reportKeyResults) {
			
			Collection<String> apps = EnvironmentsFilterInput.getApplications(apiClient,
				getSettingsData(serviceId), serviceId, name, true, true);
			
			String result = String.join(", ", apps);		

			return result;		
		}
	}
	
	protected static class KeyOutputEventVolume {
		protected long volume;
		protected int count;
		protected double rate;
	}
	
	protected static class ReportKeyOutput {
		
		protected ReportKey reportKey;
		protected RegressionKeyData regressionData;
		protected Map<TransactionKey, TransactionData> transactionMap;
		protected RegressionInput transactionRegInput;

		
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
		
		protected double score;

		protected int severeSlowdowns;
		protected String slowDownsDesc; 
		protected String regressionsDesc; 
		protected String newIssuesDesc; 
		
		protected String description; 
		protected String scoreDesc;
		
		protected KeyOutputEventVolume volumeData;
		
		protected ReportKeyFailures failures;
		protected ReportKeyTransactions transactions;
		protected ReportKeyReliability relability;
			
		
		protected ReportKeyResults(ReportKeyOutput output) {
			this.output = output;	
		}
	}
	
	protected static class ReportKeyAlerts {
		protected boolean hasNewAlert;
		protected boolean hasAnomalyAlerts;
		protected String description;
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
		
		protected Map<TransactionKey, TransactionData> transactionMap;
		protected RegressionInput regressionInput;
		
		protected SlowdownAsyncResult(ReportKey key, Map<TransactionKey, TransactionData> transactionMap,
			RegressionInput regressionInput) {
			
			super(key);
			this.transactionMap = transactionMap;
			this.regressionInput = regressionInput;
		}
	}

	public class SlowdownAsyncTask extends BaseAsyncTask implements Callable<Object> {
		
		protected String viewId;
		protected ReportKey reportKey;
		protected String serviceId;
		protected RegressionsInput input;
		protected Pair<DateTime, DateTime> timeSpan;
		protected boolean updateEvents;

		protected SlowdownAsyncTask(String serviceId, String viewId,  
				ReportKey key, RegressionsInput input,
				Pair<DateTime, DateTime> timeSpan, boolean updateEvents) {

			this.reportKey = key;
			this.serviceId = serviceId;
			this.input = input;
			this.timeSpan = timeSpan;
			this.viewId = viewId;
			this.updateEvents = updateEvents;
		}

		@Override
		public Object call() {

			beforeCall();

			try {

				RegressionFunction regressionFunction = new RegressionFunction(apiClient, settingsMaps);
				Pair<RegressionInput, RegressionWindow> regPair = regressionFunction.getRegressionInput(serviceId, 
					viewId, input, timeSpan, false);
				
				if (regPair == null) {
					return new SlowdownAsyncResult(reportKey, Collections.emptyMap(), null);
				}
				
				RegressionInput regressionInput = regPair.getFirst();
				RegressionWindow regressionWindow = regPair.getSecond();

				DateTime from = regressionWindow.activeWindowStart; 
				DateTime to = regressionWindow.activeWindowStart.plusMinutes(regressionWindow.activeTimespan);
				
				Pair<DateTime, DateTime> activeWindow = Pair.of(from, to);
				
				Collection<TransactionGraph> transactionGraphs = getTransactionGraphs(input,
					serviceId, viewId, activeWindow, input.getSearchText());
								
				TransactionDataResult transactionDataResult = getTransactionDatas(
					transactionGraphs, serviceId, viewId, timeSpan, input, true, updateEvents, false, 
					false);				
				
				SlowdownAsyncResult result;
				
				if (transactionDataResult != null) {
					result = new SlowdownAsyncResult(reportKey, transactionDataResult.items, regressionInput);
				} else {
					result = new SlowdownAsyncResult(reportKey, Collections.emptyMap(), regressionInput);
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
		protected boolean newOnly;

		protected RegressionAsyncTask(RegressionFunction function, ReportKey key, String serviceId, RegressionsInput input,
				Pair<DateTime, DateTime> timeSpan, boolean newOnly) {

			this.function = function;
			this.reportKey = key;
			this.serviceId = serviceId;
			this.input = input;
			this.timeSpan = timeSpan;
			this.newOnly = newOnly;
		}

		@Override
		public Object call() {

			beforeCall();

			try {

				RegressionOutput output = function.runRegression(serviceId, input, newOnly); 
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
				
				if (viewId != null) {
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
	
	protected class ReportKeyReliability {
		protected ReliabilityState failRateState;
		protected ReliabilityState scoreState;
		protected ReliabilityState reliabilityState;
		protected double failureRateDelta;
		protected double failRate;	
		protected String failureRateDesc;
		protected String reliabilityDesc;

	}
	
	protected class ReportKeyTransactions {
		protected long transactionVolume;
		protected long baseTransactions;
		protected String deltaDesc;
		protected long errorVolume;
		protected double avgTimeDelta;
		protected String responseValue;	
	}
	
	protected static class ReportKeyFailures {
		protected long failures;
		protected long baseFailures;
		protected long eventCount;
	}
	
	protected class ReportRow {
		
		protected List<String> fields;
		protected List<Object> values;
		
		protected ReportRow(List<String> fields) {
			this.fields = fields;
			values = Arrays.asList(new Object[fields.size()]);
		}
		
		protected void set(String field, Object value) {
				
			int index = fields.indexOf(field);
				
			if (index != -1) {
				values.set(index, value);
			}
		}
	}
	

	public ReliabilityReportFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private ReliabilityReportInput getInput(ReliabilityReportInput reportInput, 
		String serviceId, String name, boolean mustCopy) {
		
		ReportMode mode = reportInput.getReportMode();
		
		if ((mode == ReportMode.Default) && (!mustCopy)) {
			return reportInput;
		}
		
		Gson gson = new Gson();
		String json = gson.toJson(reportInput);
		ReliabilityReportInput result = gson.fromJson(json, ReliabilityReportInput.class);

		if (mode == ReportMode.Default) {
			return result;
		}
		
		switch (mode) {
		
			case Deployments:
				result.deployments = name;
				break;
				
			case Tiers_Extended:
			case Tiers:
				List<String> types = new ArrayList<String>();
				Collection<String> inputTypes = getTypes(serviceId, reportInput);
				
				if (inputTypes != null) {
					for (String type : inputTypes) {
						
						if (EventFilter.isExceptionFilter(type)) {
							types.add(type);
						} else if (GroupSettings.isGroup(type)) {
							continue;
						} else {
							types.add(type);
						}
					}
				}
				
				types.add(GroupSettings.toGroupName(name));
					
				result.types = String.join(GrafanaFunction.ARRAY_SEPERATOR_RAW, types);
				break;
			
			case Applications:
			case Apps_Extended:
				result.applications = name;
				break;
			
			default:
				break;	
		}

		return result;
	}

	@Override
	protected List<String> getColumns(EventsInput input) {
		
		ReliabilityReportInput rrInput = (ReliabilityReportInput)input;
		
		switch (rrInput.getReportMode()) {
			
			case Tiers:
			case Applications:
				return ReliabilityReportInput.DEFAULT_APP_FIELDS;
				
			case Apps_Extended:
			case Timeline_Extended:
			case Tiers_Extended:
				return ReliabilityReportInput.DEFAULT_EXTENDED_FIELDS;
			
			case Deployments:
				return ReliabilityReportInput.DEFAULT_DEP_FIELDS;
			
			case Timeline:
				return ReliabilityReportInput.DEFAULT_TIMELINE_FIELDS;

			default:
				return ReliabilityReportInput.DEFAULT_APP_FIELDS;
			
		}
	}

	private Collection<ReportKey> getTiers(String serviceId, ReliabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan) {
 		
		Collection<String> keyTiers = getSettings(serviceId).getTierNames();
		
		Collection<String> types = getTypes(serviceId, input);
		
		Set<ReportKey> result = new TreeSet<ReportKey>();
		
		if (!CollectionUtil.safeIsEmpty(types)) {
						
			for (String type : types) {
				
				if (EventFilter.isExceptionFilter(type)) {
					continue;
				}
				
				if (GroupSettings.isGroup(type)) {
					String name = GroupSettings.fromGroupName(type);
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
			
			List<Category> categories = getSettingsData(serviceId).tiers;
			
			for (Category category : categories)  {
				
				if ((category.getType() == CategoryType.infra)
				&& (!CollectionUtil.safeIsEmpty(category.names))) {
				
					for (String label : category.labels) {
						result.add(new ReportKey(label, true));
					}
				}
			}
		} 
		
		if ((input.limit != 0) && (result.size() >= input.limit)) {
			return result;
		}
		
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timeSpan.getFirst(), 
			timeSpan.getSecond(), null);
		
		if (eventsMap == null) {
			return Collections.emptyList();
		}
		
		Categories categories = getSettings(serviceId).getCategories();
				
		for (EventResult event : eventsMap.values()) {
						
			
			boolean is3rdPartyCode = false;
			
			if (event.error_origin != null) {
				
				Set<String> originLabels = categories.getCategories(
					event.error_origin.class_name, CategoryType.infra);
				
				if (!CollectionUtil.safeIsEmpty(originLabels)) {
					result.addAll(toReportKeys(originLabels, false));
					is3rdPartyCode = true; 
				}
			}
			
			if (event.error_location != null) {
				
				Set<String> locationLabels = categories.getCategories(
					event.error_location.class_name, CategoryType.infra);
				
				if (!CollectionUtil.safeIsEmpty(locationLabels)) {
					result.addAll(toReportKeys(locationLabels, false));
					is3rdPartyCode = true; 
				}
			}
			
			if ((input.addAppTier) && (!is3rdPartyCode)) {
				result.add(new ReportKey(EventFilter.APP_CODE, false));
			}
		}
		
		return result;
	}

	private List<VolumeOutput> getAppVolumes(String serviceId, ReliabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan, Collection<String> apps) {

		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();

		for (String app : apps) {
			RegressionsInput appInput = getInput(input, serviceId, app, false);
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

	protected static class RegressionKeyData {

		protected ReportKey reportKey;
		protected RegressionOutput regressionOutput;

		protected RegressionKeyData(ReportKey key,
				RegressionOutput regressionOutput) {

			this.reportKey = key;
			this.regressionOutput = regressionOutput;
		}
	}

	protected List<ReportAsyncResult> processAsync(String serviceId, ReliabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan, Collection<ReportKey> reportKeys) {

		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
		List<ReportAsyncResult> result = new ArrayList<ReportAsyncResult>();
				
		ScoreType scoreType = input.getScoreType();
		
		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null) {
			return Collections.emptyList();
		}
		
		for (ReportKey reportKey : reportKeys) {
			
			RegressionFunction regressionFunction = new RegressionFunction(apiClient, settingsMaps);

			RegressionsInput regressionsInput = getInput(input, serviceId, reportKey.name, false);
			RegressionsInput transactionInput = getInput(input, serviceId, reportKey.name, true);
			
			if ((scoreType == ScoreType.Combined) 
			|| (scoreType == ScoreType.Regressions)) {

				RegressionOutput regressionOutput = ApiCache.getRegressionOutput(apiClient, 
					serviceId, regressionsInput, regressionFunction, false, false);
						
				if (regressionOutput != null) {
					result.add(new RegressionAsyncResult(reportKey, regressionOutput));
				} else {
					tasks.add(new RegressionAsyncTask(regressionFunction, 
						reportKey, serviceId, regressionsInput, timeSpan, false));
				}
			}
			
			if ((scoreType == ScoreType.Combined) 
			|| (scoreType == ScoreType.Slowdowns)) {
				tasks.add(new SlowdownAsyncTask(serviceId, viewId,  
					reportKey, transactionInput, timeSpan, input.getReportMode() == ReportMode.Apps_Extended));
			}
			
			if (scoreType == ScoreType.NewOnly) {

				RegressionOutput regressionOutput = ApiCache.getRegressionOutput(apiClient, 
					serviceId, regressionsInput, regressionFunction, true, false);
						
				if (regressionOutput != null) {
					result.add(new RegressionAsyncResult(reportKey, regressionOutput));
				} else {
					tasks.add(new RegressionAsyncTask(regressionFunction, 
						reportKey, serviceId, regressionsInput, timeSpan, true));
				}
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

	private Collection<ReportKey> getActiveApplications(String serviceId, ReliabilityReportInput input,
			Pair<DateTime, DateTime> timeSpan) {

		List<String> keyApps = new ArrayList<String>();
		
		GroupSettings appGroups = getSettingsData(serviceId).applications;
		
		if (appGroups != null) {
			keyApps.addAll(appGroups.getAllGroupNames(true));
		}
		
		List<ReportKey> reportKeys;
		
		Collection<String> selectedApps = input.getApplications(apiClient,
			getSettingsData(serviceId), serviceId, false, true);
		
		if (!CollectionUtil.safeIsEmpty(selectedApps)) {
			
			reportKeys = new ArrayList<ReportKey>();
			
			for (String selectedApp : selectedApps) {
				boolean isKey = keyApps.contains(selectedApp);
				reportKeys.add(new AppReportKey(selectedApp, isKey));
			}
			
			return reportKeys;
		}
			
		if (keyApps.size() > input.limit) {
			return toAppReportKeys(keyApps, true);
		}
				
		List<String> activeApps = new ArrayList<String>(ApiCache.getApplicationNames(apiClient, serviceId, true));
		
		List<Category> categories = getSettingsData(serviceId).tiers;
		 
		for (Category category : categories) {
			
			if (category.getType() != CategoryType.app) {
				continue;
			}
			
			if (CollectionUtil.safeIsEmpty(category.names)) {
				continue;
			}
			
			for (String name : category.labels) {
				String appName = EnvironmentsFilterInput.toAppLabel(name);
				activeApps.add(appName);
			}
		}
		
		int appSize = keyApps.size() + activeApps.size();
		
		if ((appSize > 0) && (appSize < input.limit)) {
			
			reportKeys = new ArrayList<ReportKey>(keyApps.size() + activeApps.size());
			
			reportKeys.addAll(toAppReportKeys(keyApps, true));
			reportKeys.addAll(toAppReportKeys(activeApps, false));
			
			reportKeys.sort(null);
			
			return reportKeys;
		}
		
		Collection<String> nonKeyApps;
		
		if (input.queryAppVolumes) {
			List<VolumeOutput> appVolumes = getAppVolumes(serviceId, input, timeSpan, activeApps);
			nonKeyApps = limitVolumes(appVolumes, input.limit);
		} else {
			List<String> sortedActiveApps = new ArrayList<String>(activeApps);
			sortApplicationsByProcess(serviceId, sortedActiveApps,
				input.getServers(serviceId), input.getDeployments(serviceId, apiClient));			
			nonKeyApps =  sortedActiveApps; 
		}
		
		reportKeys = new ArrayList<ReportKey>();
		
		reportKeys.addAll(toAppReportKeys(keyApps, true));
		reportKeys.addAll(toAppReportKeys(nonKeyApps, false));
		
		List<ReportKey> result;
		
		if (reportKeys.size() > input.limit) {
			result = reportKeys.subList(0, input.limit);
		} else {
			result = reportKeys;
		}
		
		result.sort(null);
		
		return result;
	}

	private static void sortDeployments(List<SummarizedDeployment> deps) {
		
		deps.sort(new Comparator<SummarizedDeployment>() {

			@Override
			public int compare(SummarizedDeployment o1, SummarizedDeployment o2) {
				
				if ((o1.first_seen == null) && (o2.first_seen == null)) {
					return DeploymentUtil.compareDeployments(o2.name, o1.name);
				}
				
				int hasFirstSeen = Boolean.compare(o2.first_seen != null, o1.first_seen != null);
				
				if (hasFirstSeen != 0) {
					return hasFirstSeen;
				}
				
				DateTime l1 = TimeUtil.getDateTime(o1.first_seen);
				DateTime l2 = TimeUtil.getDateTime(o2.first_seen);

				int firstSeenCompare = Long.compare(l2.getMillis(), l1.getMillis());
				
				if (firstSeenCompare != 0) {
					return firstSeenCompare;
				}
				
				int nameCompare = DeploymentUtil.compareDeployments(o1.name, o2.name);
				
				return nameCompare;
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
	
	private Collection<ReportKey> toAppReportKeys(Collection<String> keys, boolean isKey) {
		
		List<ReportKey> result = new ArrayList<ReportKey>(keys.size());
		
		for (String key : keys) {
			result.add(new AppReportKey(key, isKey));
		}
		
		return result;
	}
	
	private List<ReportKey> getSelectedDeployments(
		Collection<String> selectedDeployments, Collection<SummarizedDeployment> allDeps) {
		
		List<ReportKey> result = new ArrayList<ReportKey>();
						
		for (String selectedDeployment : selectedDeployments) {
			
			SummarizedDeployment curr = null;
	
			if (allDeps != null) {
					
				for (SummarizedDeployment dep : allDeps) {
										
					if (dep.name.equals(selectedDeployment)) {
						curr = dep;
						break;
					}
				}
			}
			
			result.add(new DeploymentReportKey(selectedDeployment, false, curr));
		}
		
		return result;
	}
	
	private List<ReportKey> getLiveDeployments(String serviceId, 
			ReliabilityReportInput input, Pair<DateTime, DateTime> timespan,
			Pair<Gson, String> gsonPair) {
	
		List<ReportKey> result = new ArrayList<ReportKey>();
		Collection<SummarizedDeployment> activeDeps = DeploymentUtil.getDeployments(apiClient, serviceId, true);
		
		boolean hasApps = input.hasApplications();
		
		if (activeDeps != null) {
			
			List<SummarizedDeployment> sortedActive = new ArrayList<SummarizedDeployment>(activeDeps);
			sortDeployments(sortedActive);
	
			for (int i = 0; i < Math.min(input.limit, sortedActive.size()); i++) {
				
				SummarizedDeployment activeDep = sortedActive.get(i);
				
				if (activeDep.last_seen != null) {
					DateTime firstSeen = TimeUtil.getDateTime(activeDep.last_seen);
					if (firstSeen.isBefore(timespan.getFirst())) {
						continue;
					}
				}
				
				if ((hasApps) && (!appHasDeployVolume(serviceId, 
					gsonPair, timespan, activeDep.name))) {
					continue;
				}
				
				result.add(new DeploymentReportKey(activeDep.name, true, activeDep));
			}
		}
		
		return result;
	}
	
	void addNonActiveDeployments(List<ReportKey> output, 
		ReliabilityReportInput input, Pair<DateTime, DateTime> timespan,
		Collection<SummarizedDeployment> allDeps ) {
		
		if (allDeps == null) {
			return;
		}
		
		for (SummarizedDeployment dep : allDeps) {
				
			boolean canAdd = true;
				
			if (dep.last_seen != null) {
				DateTime lastSeen = TimeUtil.getDateTime(dep.last_seen);
				if (lastSeen.isBefore(timespan.getFirst())) {
					canAdd = false;					}
			}
				
			boolean isLive = false;
				
			if (dep.last_seen != null) {
				DateTime lastSeen = TimeUtil.getDateTime(dep.last_seen);
				isLive = lastSeen.plusHours(1).isAfter(timespan.getSecond());
			}
								
			DeploymentReportKey key = new DeploymentReportKey(dep.name, isLive, dep);
				
			int keyIndex = output.indexOf(key);
				
			if (keyIndex == -1) {
					
				if (canAdd) {
					output.add(key);
				}

			} 
				
			if (output.size() >= input.limit) {
				break;
			}
		}		
	}
	
	private void updateCompareDeployments(List<ReportKey> output, 
		String serviceId, ReliabilityReportInput input, Pair<DateTime, DateTime> timespan,
		List<SummarizedDeployment> sortedDeps, Pair<Gson, String> gsonPair) {
		
		boolean hasApps = input.hasApplications();
	
		for (ReportKey reportKey : output) {
						
			for (int i = 0; i < sortedDeps.size(); i++) {
				
				SummarizedDeployment dep = sortedDeps.get(i);			

				if (dep.name.equals(reportKey.name)) {
					
					if (i < sortedDeps.size() - 1) {
						
						DeploymentReportKey depReportKey = (DeploymentReportKey)reportKey;
						SummarizedDeployment compareToDep;
						
						if (hasApps) {
							compareToDep = findPrevDeployment(serviceId, 
								timespan, gsonPair, sortedDeps, i);
						} else {
							compareToDep = sortedDeps.get(i + 1);
						}
						
						depReportKey.compareToDep = compareToDep;
					}
					
					break;
				}				
			}
		}	
	}
	
	private SummarizedDeployment findPrevDeployment(String serviceId, 
		Pair<DateTime, DateTime> timespan, Pair<Gson, String> gsonPair, 
		List<SummarizedDeployment> depsList, int index) {
		
		for (int tryIndex = 0; tryIndex < MAX_FIND_DEP_TRIES; tryIndex++) {
			
			int prevIdex = index + tryIndex + 1;
			
			if (prevIdex > depsList.size() - 1) {
				break;
			}
			
			SummarizedDeployment prevDep = depsList.get(prevIdex);
			
			if (appHasDeployVolume(serviceId, gsonPair, timespan, prevDep.name)) {
				return prevDep;
			}
		}
		
		return null;
	}
		
	private Collection<ReportKey> getActiveDeployments(String serviceId, 
		ReliabilityReportInput input, Pair<DateTime, DateTime> timespan) {

		List<ReportKey> result = new ArrayList<ReportKey>();
		Collection<String> selectedDeployments = input.getDeployments(serviceId, apiClient);
		
		Collection<SummarizedDeployment> allDeps = DeploymentUtil.getDeployments(apiClient, serviceId, false);
		List<SummarizedDeployment> sortedDeps = new ArrayList<SummarizedDeployment>(allDeps);
		
		sortDeployments(sortedDeps);
	
		Gson gson = new Gson();
		String json = gson.toJson(input);
		Pair<Gson, String> gsonPair = Pair.of(gson, json);
		
		if (!CollectionUtil.safeIsEmpty(selectedDeployments)) {
			result = getSelectedDeployments(selectedDeployments, allDeps);
		} else {
			
			result = getLiveDeployments(serviceId, input, timespan, gsonPair);
			
			if ((!input.liveDeploymentsOnly) && (input.limit - result.size() > 0)) {
				addNonActiveDeployments(result, input, timespan, sortedDeps);	
			}
		}
		
		updateCompareDeployments(result, serviceId, input, timespan, 
			sortedDeps, gsonPair);

		return result;
	}
		
	private Collection<ReportKey> getActiveKeys(String serviceId, ReliabilityReportInput regInput,
			Pair<DateTime, DateTime> timeSpan) {
		
		Collection<ReportKey> keys;
			
		ReportMode mode = regInput.getReportMode();
		
		switch (mode) {
				
			case Deployments:
				keys = getActiveDeployments(serviceId, regInput, timeSpan);
				break;
					
			case Tiers: 
			case Tiers_Extended:
				keys = getTiers(serviceId, regInput, timeSpan);
				break;
				
			case Apps_Extended: 
			case Applications: 
				keys = getActiveApplications(serviceId, regInput, timeSpan);
				break;
					
			case Default: 
				keys = Collections.singleton(getDefaultReportKey(serviceId, regInput));
				break;
					
			default: 
				throw new IllegalStateException("Unsopported mode " + mode);
			}

		return keys;
	}
	
	private ReportKey getDefaultReportKey(String serviceId, ReliabilityReportInput regInput) {
		
		boolean isKey = false;

		if (regInput.applications != null) {
			
			GroupSettings appGroups = getSettingsData(serviceId).applications;
			
			if (appGroups != null) {
				 
				List<String> keyApps = new ArrayList<String>(appGroups.getAllGroupNames(true));
				Collection<String> apps = regInput.getApplications(apiClient, getSettingsData(serviceId), 
					serviceId, false, true);
				
				isKey = false;
				
				for (String app : apps) {
					
					if (keyApps.contains(app)) {
						isKey = true;
						break;
					}
				}
			}
		}
			
		return new ReportKey(DEFAULT_KEY, isKey);
	}
	
	private static void addDeduction(String name, int value, double weight, List<String> deductions) {
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
				
				if ((int)weight != weight) {
					builder.append(weight);
				} else {
					builder.append((int)weight);
				}
			}
		}
		
		deductions.add(builder.toString());
	}
	
	private static int getRegressionScoreWindow(RegressionOutput regressionOutput) {
		
		int result = 0;
		
		if (regressionOutput.empty) {
			return 0;
		}
		
		if (CollectionUtil.safeIsEmpty(regressionOutput.regressionInput.deployments)) {
			result = regressionOutput.regressionInput.activeTimespan;
		} else {
						
			DateTime lastPointTime = null;
			
			for (int i = regressionOutput.activeVolumeGraph.points.size() - 1; i >= 0; i--) {
				
				GraphPoint gp = regressionOutput.activeVolumeGraph.points.get(i);
				
				if ((gp.stats != null) && 
					((gp.stats.invocations > 0) || (gp.stats.hits > 0))) {
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
	
	private static double getReportKeyWeight(RegressionReportSettings reportSettings, 
			ReportMode reportMode, boolean isKey) {
		
		if (reportMode == ReportMode.Deployments) {
			return reportSettings.score_weight;	
		}
		
		if ((isKey) && (reportSettings.key_score_weight > 0)) {
			return reportSettings.key_score_weight;
		} else {
			return reportSettings.score_weight;	
		}		
	}
	
	public static Pair<Double, Integer> getScore(
			RegressionOutput regressionOutput, RegressionReportSettings reportSettings, 
			int newEvents, int severeNewEvents, int regressions, int severeRegressions,
			int slowdowns, int severeSlowdowns, boolean isKey, boolean deductFrom100,
			ReportMode reportMode, int appSize) {
		
		double newEventsScore = newEvents * reportSettings.new_event_score;
		double severeNewEventScore = severeNewEvents * reportSettings.severe_new_event_score;
		
		double criticalRegressionsScore = severeRegressions  * reportSettings.critical_regression_score;
		double regressionsScore = regressions * reportSettings.regression_score;
		
		if ((reportMode != ReportMode.Tiers) 
		&& (reportMode != ReportMode.Tiers_Extended)) {
			criticalRegressionsScore += severeSlowdowns * reportSettings.critical_regression_score;
			regressionsScore += slowdowns * reportSettings.regression_score;
		}
		
		int scoreWindow = getRegressionScoreWindow(regressionOutput);		
		double scoreDays = Math.max(1, (double)scoreWindow / 60 / 24);
	
		double weight = getReportKeyWeight(reportSettings, reportMode, isKey);
		int appFactor = Math.max(1, appSize);
		
		double rawScore = (newEventsScore + severeNewEventScore + criticalRegressionsScore + regressionsScore) / scoreDays / appFactor;
		
		double resultScore;
				
		if (deductFrom100) {
			resultScore = Math.max(100 - (weight * rawScore), 0);
		} else {
			resultScore = Math.max(weight * rawScore, 0);
		}	
		
		return Pair.of(resultScore, scoreWindow);
	}
	
	private Pair<Double, Integer> getScore(String serviceId,
		ReliabilityReportInput input, RegressionReportSettings reportSettings, 
		ReportKeyResults reportKeyResults) {
		
		RegressionOutput regressionOutput = reportKeyResults.output.regressionData.regressionOutput;
		
		ReportMode reportMode = input.getReportMode();	
		
		int keyWeight = reportKeyResults.output.reportKey.getWeight(serviceId);
		
		return getScore(regressionOutput, reportSettings, 
				regressionOutput.newIssues, regressionOutput.severeNewIssues, 
				regressionOutput.regressions, regressionOutput.criticalRegressions, 
				reportKeyResults.slowdowns, reportKeyResults.severeSlowdowns, 
				reportKeyResults.output.reportKey.isKey,true,reportMode, keyWeight);

	}
	
	private String getScoreDescription(String serviceId, ReliabilityReportInput input,
		RegressionReportSettings reportSettings,
		ReportKeyResults reportKeyResults, double resultScore, int period) {
		
		StringBuilder result = new StringBuilder();

		String duration = prettyTime.formatDuration(new DateTime().minusMinutes(period).toDate());
		
		RegressionOutput regressionOutput = reportKeyResults.output.regressionData.regressionOutput;

		result.append("Score for ");

		ReportMode reportMode = input.getReportMode();

		String fullName = reportKeyResults.output.reportKey.getFullName(serviceId, reportKeyResults);
		result.append(fullName);	
		
		result.append(" = 100");
		
		int allIssuesCount = regressionOutput.newIssues + regressionOutput.severeNewIssues + 
				regressionOutput.criticalRegressions + regressionOutput.regressions;
		
		if ((reportMode != ReportMode.Tiers) &  (reportMode != ReportMode.Tiers_Extended)) {
			allIssuesCount += reportKeyResults.slowdowns + reportKeyResults.severeSlowdowns;
		}
		
		if (allIssuesCount > 0) {
			result.append(" - (");
			
			List<String> deductions = new ArrayList<String>();
			
			addDeduction("new issue", regressionOutput.newIssues, reportSettings.new_event_score, deductions);
			addDeduction("severe new issue", regressionOutput.severeNewIssues, reportSettings.severe_new_event_score, deductions);
			addDeduction("error increase", regressionOutput.regressions, reportSettings.regression_score, deductions);
			addDeduction("severe error increase", regressionOutput.criticalRegressions, reportSettings.critical_regression_score, deductions);
			
			if ((reportMode != ReportMode.Tiers) 
			 && (reportMode != ReportMode.Tiers_Extended)) {
				addDeduction("slowdown", reportKeyResults.slowdowns, reportSettings.regression_score, deductions);
				addDeduction("severe slowdown", reportKeyResults.severeSlowdowns, reportSettings.critical_regression_score, deductions);
			}
			
			double weight = getReportKeyWeight(reportSettings, reportMode, reportKeyResults.output.reportKey.isKey);
			
			String deductionString = String.join(" + ", deductions);
			result.append(deductionString);
			result.append(") * ");
			result.append(weight);			
			result.append(", avg over ");
			result.append(duration);
			
			int appsSize = reportKeyResults.output.reportKey.getWeight(serviceId);
			
			if (appsSize > 1) {
				result.append(" and ");
				result.append(appsSize);
				result.append(" apps");
			} 
			
			result.append(" = ");
						
			if ((int)resultScore != resultScore) {
				result.append(decimalFormat.format(resultScore));
			} else {
				result.append((int)resultScore);
			}	
		}
		
		return result.toString(); 
	}
	
	protected Collection<ReportKeyOutput> executeTimeline(String serviceId,
		ReliabilityReportInput regInput, Pair<DateTime, DateTime> timeSpan) {
		
		ReliabilityKpiGraphFunction kpiGraph = new ReliabilityKpiGraphFunction(apiClient, settingsMaps);
		
		Gson gson = new Gson();
		String json = gson.toJson(regInput);
		ReliabilityKpiGraphInput pkInput = gson.fromJson(json, ReliabilityKpiGraphInput.class);
		
		pkInput.kpi = ReliabilityKpi.Score.toString();
		pkInput.reportInterval = regInput.reportInterval; 
		pkInput.aggregate = false;
		pkInput.limit = regInput.limit;
		
		Collection<TaskKpiResult> taskResults = kpiGraph.executeIntervals(Collections.singleton(serviceId), 
			pkInput, timeSpan);
		
		Collection<ReportKeyOutput> result = getTimelineResults(taskResults);
		
		return result;
	}
	
	private Collection<ReportKeyOutput> getTimelineResults(Collection<TaskKpiResult> taskResults) {
		
		List<Pair<String, KpiInterval>> sortedKpis = new ArrayList<Pair<String, KpiInterval>>();

		for (TaskKpiResult taskKpiResult : taskResults) {
			
			for (KpiInterval kpiInterval : taskKpiResult.kpiIntervals) {
				sortedKpis.add(Pair.of(taskKpiResult.app, kpiInterval));
			}
		}
		
		sortedKpis.sort(new Comparator<Pair<String, KpiInterval>>() {

			@Override
			public int compare(Pair<String, KpiInterval> o1, Pair<String, KpiInterval> o2) {
				
				if (Objects.equal(o1.getFirst(), o2.getFirst())) {
					return o2.getSecond().period.getSecond().compareTo(o1.getSecond().period.getSecond());
				}
				
				if (o1.getFirst() == null) {
					return -1;
				}
				
				if (o2.getFirst() == null) {
					return -1;
				}
				
				return 0;
			}
		});
		
		List<ReportKeyOutput> result = new ArrayList<ReportKeyOutput>();
		
		for (Pair<String, KpiInterval> pair : sortedKpis) {
				
			String app = pair.getFirst();
			KpiInterval kpiInterval  = pair.getSecond();
			
			String intervalName = formatInterval(kpiInterval.period);
			String reportKeyName;
				
			if ((app != null) && (app.length()) > 0) {					
				reportKeyName = app + " " + intervalName;
			} else {
				reportKeyName = intervalName;
			}
				
			ReportKey reportKey = new ReportKey(reportKeyName, false);
			ReportKeyOutput reportKeyOutput = new ReportKeyOutput(reportKey);
				
			if (kpiInterval instanceof ScoreInterval) {
				
				ScoreInterval scoreInterval = (ScoreInterval)kpiInterval;
								 
				if ((scoreInterval.slowdownInterval != null)
				&& (scoreInterval.slowdownInterval.transactionMap != null)) {
					reportKeyOutput.transactionMap = scoreInterval.slowdownInterval.transactionMap;
					reportKeyOutput.transactionRegInput =  scoreInterval.slowdownInterval.regressionInput;
				} 
				
				RegressionOutput regressionOutput;
				
				if ((scoreInterval.regressionInterval != null) 
				&& (scoreInterval.regressionInterval.output != null)) {
					regressionOutput = scoreInterval.regressionInterval.output;
				} else {
					regressionOutput = RegressionOutput.emptyOutput;
				}
				
				reportKeyOutput.regressionData = new RegressionKeyData(reportKey, regressionOutput);
			}
				
			result.add(reportKeyOutput);	
		}
		
		return result;
	}
	
	private String formatInterval(Pair<DateTime, DateTime> period) {
				
		String result;
		
		DateTime dateTime = period.getFirst();
		Date date = dateTime.toDate();
		if (dateTime.isAfter(TimeUtil.now().minusDays(7))) {
			result = singleDayDateformat.format(date);
		} else {
			result = dayInMonthDateformat.format(date);
		}	
		
		return result; 
	}
	
	protected Collection<ReportKeyOutput> executeReport( 
		String serviceId, ReliabilityReportInput regInput,
		Pair<DateTime, DateTime> timeSpan) {
		
		ReportMode reportMode = regInput.getReportMode();
		
		if ((reportMode == ReportMode.Timeline) 
		|| (reportMode == ReportMode.Timeline_Extended)) {
			return executeTimeline(serviceId, regInput, timeSpan);
		}
		
		Collection<ReportKey> keys = getActiveKeys(serviceId, regInput, timeSpan);
		
		logger.debug("Executing report " + reportMode + " keys: " + Arrays.toString(keys.toArray()));

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
	
				if ((regressionOutput == null)
				|| (regressionOutput.rateRegression == null)) {
					continue;
				}
				
				RegressionKeyData regressionData = new RegressionKeyData(asyncResult.key, 
						regressionOutput);
				
				reportKeyOutput.regressionData = regressionData;
			}
			
			if (asyncResult instanceof SlowdownAsyncResult) {
				
				SlowdownAsyncResult slowdownAsyncResult = (SlowdownAsyncResult)asyncResult;
				reportKeyOutput.transactionMap = slowdownAsyncResult.transactionMap;
				reportKeyOutput.transactionRegInput = slowdownAsyncResult.regressionInput;
			}
			
		}
		
		boolean sortAsc = getSortedAsc(regInput.getSortType(), true);		
		List<ReportKeyOutput> result = new ArrayList<ReportKeyOutput>(reportKeyOutputMap.values());

		if (reportMode == ReportMode.Deployments) {
			sortDeploymentKeys(result, sortAsc);
		} else {	
			sortKeysByName(result, sortAsc);
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
				throw new IllegalStateException(String.valueOf(sortType));
		}
	}
	
	private void sortDeploymentKeys(List<ReportKeyOutput> scores, boolean asc) {
		
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
	
	private void sortKeysByName(List<ReportKeyOutput> scores, boolean asc) {
		
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

		if (!(functionInput instanceof ReliabilityReportInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		ReliabilityReportInput input = (ReliabilityReportInput)getInput((ViewInput)functionInput);

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
			case SingleStatDesc:
				return processSingleStat(input);	
			default:
				throw new IllegalStateException("Unsupported render mode " + input.render); 
		}
	}
	
	private Object getServiceSingleStat(Collection<String> serviceIds, String serviceId, Pair<DateTime, DateTime> timeSpan, ReliabilityReportInput input) {
		Collection<ReportKeyOutput> reportKeyOutputs = executeReport(serviceId, input, timeSpan);
		Collection<ReportKeyResults> reportKeyResults = getReportResults(serviceId, timeSpan, input, reportKeyOutputs);
		
		Object result;
		
		if (reportKeyResults.size() > 0) {
			ReportKeyResults reportKeyResult = reportKeyResults.iterator().next();
			result = getReportKeyValue(serviceIds, serviceId, timeSpan, input, reportKeyResult);
		} else {
			result = 0;
		}
		
		return result;
	}
	
	private Object getSingleStat(Collection<String> serviceIds, Pair<DateTime, DateTime> timeSpan, 
		ReliabilityReportInput input)
	{	
		if (CollectionUtil.safeIsEmpty(serviceIds)) {
			return 0;
		}
		
		List<Object> serviceValues = new ArrayList<Object>(serviceIds.size());
		
		for (String serviceId : serviceIds) {
			Object serviceValue = getServiceSingleStat(serviceIds, serviceId, timeSpan, input);
			serviceValues.add(serviceValue);
		}
		
		Object result;
		
		if (serviceValues.size() == 1) {
			result = serviceValues.iterator().next();
		} else {
			int count = 0;
			double sum = 0;

			for (Object serviceValue : serviceValues) {
				
				if (serviceValue instanceof Double) {
					sum += (Double)serviceValue;
					count++;
				}
			}
			
			if (count > 0) {
				result = sum / count;
			} else {
				result = 0;
			}
		}
		
		return result;
	}

	private List<Series> processSingleStat(ReliabilityReportInput input) {
		Collection<String> serviceIds = getServiceIds(input);
		
		if (CollectionUtil.safeIsEmpty(serviceIds)) {
			return Collections.emptyList();
		}
		
		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		Object singleStatText = getSingleStat(serviceIds, timeSpan, input);
		
		return createSingleStatSeries(timeSpan, singleStatText);
	}

	private static Object formatIssues(ReliabilityReportInput input, int nonSevere, int severe) {
		
		Object result;
		
		if (severe > 0) {
			
			if (nonSevere == 0) {
				if (input.sevOnlyFormat != null) {
					result = String.format(input.sevOnlyFormat, severe);
				} else {
					result = String.valueOf(severe);
				}
			} else {
				if (input.sevAndNonSevFormat != null) {
					result = String.format(input.sevAndNonSevFormat, nonSevere + severe, severe);
				} else {
					result = String.valueOf(nonSevere + severe);
				}
			}
		} else {
			if (nonSevere != 0) {
				result = Integer.valueOf(nonSevere);
			} else {
				result = "";
			}
		}
		
		return result;
	}

	
		
	private Collection<ReportKeyResults> getReportResults(String serviceId, 
			Pair<DateTime, DateTime> timeSpan, ReliabilityReportInput input, 
			Collection<ReportKeyOutput> reportKeyOutputs) {
		
		RegressionReportSettings reportSettings = getSettingsData(serviceId).regression_report;
		
		if (reportSettings == null) {
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
				
				reportKeyResults.newIssuesDesc = RegressionFunction.getNewIssuesDesc(
					reportKeyOutput.regressionData.regressionOutput, RegressionsInput.MAX_TOOLTIP_ITEMS);
					
				reportKeyResults.regressionsDesc = RegressionFunction.getRegressionsDesc(
						reportKeyOutput.regressionData.regressionOutput, RegressionsInput.MAX_TOOLTIP_ITEMS);
			}
			
			if (reportKeyOutput.transactionMap != null) {
				
				Pair<Integer, Integer> slowdownPair = getSlowdowns(reportKeyOutput.transactionMap.values());
				reportKeyResults.slowdowns = slowdownPair.getFirst().intValue();
				reportKeyResults.severeSlowdowns = slowdownPair.getSecond().intValue();
				reportKeyResults.slowDownsDesc = TransactionsListFunction.getSlowdownsDesc(reportKeyOutput.transactionMap.values(), 
					null, RegressionsInput.MAX_TOOLTIP_ITEMS);
			}
			
			if (reportKeyOutput.regressionData != null) {
			
				Pair<Double, Integer> scorePair = getScore(serviceId, input, reportSettings, reportKeyResults);
				
				reportKeyResults.score = scorePair.getFirst();
				
				reportKeyResults.scoreDesc = getScoreDescription(serviceId, input, reportSettings, reportKeyResults,
						reportKeyResults.score, scorePair.getSecond());
				
				reportKeyResults.description = getDescription(reportKeyOutput.regressionData, 
					reportKeyResults.newIssuesDesc, reportKeyResults.regressionsDesc, reportKeyResults.slowDownsDesc);
					
				reportKeyResults.volumeData = getKeyOutputEventVolume(reportKeyOutput);
				 
				reportKeyResults.failures = getAppFailureData(serviceId, input, timeSpan, reportKeyResults);
				reportKeyResults.transactions = getAppTransactionData(reportKeyResults);
				
				reportKeyResults.relability = getAppReliabilityData(serviceId, reportKeyResults, 
					input, reportKeyResults.transactions, reportKeyResults.failures);
					
				
				result.add(reportKeyResults);
			}			
		}
		
		boolean isSingleStat = (input.render == RenderMode.SingleStat)
							|| (input.render == RenderMode.SingleStatDesc)
							|| (input.render == RenderMode.SingleStatVolume)
							|| (input.render == RenderMode.SingleStatVolumeText);
		
		if ((!isSingleStat) && (result.size() > input.limit)) {
			return limitByScore(result, input);
		}
		
		return result;
	}
	
	
	private int compareKeys(ReportKeyResults o1, ReportKeyResults o2) {
		
		int scoreDelta = (int)Math.round((o1.score - o2.score) * 100);
		
		int result;
		
		if (scoreDelta != 0) {
			result = scoreDelta;
		} else {
			
			double failDelta = o2.relability.failRate - o1.relability.failRate;
			
			if (failDelta > 0) {
				result = 1;
			} else if (failDelta < 0) {
				result = -1;
			} else {
				result = 0;
			}
		}
	
		return result;
	}
	
	private void sortByScore(List<ReportKeyResults> results) {
		
		results.sort(new Comparator<ReportKeyResults>()
		{
			@Override
			public int compare(ReportKeyResults o1, ReportKeyResults o2)
			{	
				ReportKey k1 = o1.output.reportKey;
				ReportKey k2 = o2.output.reportKey;
				
				int scoreDelta = compareKeys(o1, o2);

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
	}
	
	private Collection<ReportKeyResults> limitByScore(List<ReportKeyResults> results, 
			ReliabilityReportInput input) {
		
		ReportMode reportMode = input.getReportMode();
		
		boolean isTimeline = (reportMode == ReportMode.Timeline) 
						  || (reportMode == ReportMode.Timeline_Extended);
		
		if (!isTimeline) { 
			sortByScore(results);
		}
		
		return results.subList(0, input.limit);
	}
	
	private KeyOutputEventVolume getKeyOutputEventVolume(ReportKeyOutput output) {
		
		if (output.regressionData.regressionOutput.empty) {
			return null;
		}
		
		Collection<EventResult> events = output.regressionData.regressionOutput.regressionInput.events;
		
		if (events == null) {
			return null;
		}		
	
		KeyOutputEventVolume result = new KeyOutputEventVolume();

		for (EventResult event : events) {
			
			if (event.stats != null) {
				result.volume += event.stats.hits;
				result.count++;
			}	
		}
		if (result.volume > 0) {
			
			for (EventResult event : events) {
				
				if ((event.stats == null)  || (event.stats.invocations == 0)) {
					continue;
				}
					
				result.rate += (double)event.stats.hits / (double)event.stats.invocations / result.volume;
			}
		}
		
		return result;
	}
	
	private String getDescription(RegressionKeyData regressionData, String newErrorsDesc, 
		String regressionDesc, String slowdownDesc) {
		
		StringBuilder result = new StringBuilder();
		
		result.append(regressionData.reportKey);
		
		if (regressionData.regressionOutput.empty) {
			return result.toString();
		}
		
		result.append(" over ");
		
		DateTime activeWindow = regressionData.regressionOutput.regressionInput.activeWindowStart;
		result.append(". ");
		
		result.append(prettyTime.format(new Date(activeWindow.getMillis())));
		
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
	
	private EventFilter getFailureTypeFilter(String serviceId, 
		Pair<DateTime, DateTime> timespan, ReliabilityReportInput input,
		ReportKeyResults reportKeyResult) {
		
		Gson gson = new Gson();
		String json = gson.toJson(input);
		ReliabilityReportInput failureInput = gson.fromJson(json, input.getClass());
		
		ReportMode reportMode = input.getReportMode() ;
		
		switch (reportMode) {
			
			case Apps_Extended:
			case Timeline_Extended:
			case Default:
				failureInput.types = input.getFailureTypes(); 
				break;
		
			case Tiers:
			case Tiers_Extended:
				failureInput.types = GroupSettings.toGroupName(reportKeyResult.output.reportKey.name);
				break;
				
			default: return null;
		}
		
		EventFilter result = getEventFilter(serviceId, failureInput, timespan);
		
		return result;
	}
	
	private ReliabilityState getReliabilityState(RegressionInput regressionInput, double deltaValue) {
		
		ReliabilityState result;
		
		if (deltaValue > regressionInput.criticalRegressionDelta) {
			result = ReliabilityState.Severe;
		} else if (deltaValue > regressionInput.regressionDelta) {
			result = ReliabilityState.Warning;
		} else {
			result = ReliabilityState.OK;
		}

		return result;
		
	}
	
	private Pair<ReliabilityState, Double> getReliabilityState(double baseAvg, double avg,  
		long volume, RegressionInput regressionInput) {
				
		ReliabilityState state;
		double deltaValue;
		
		if ((baseAvg > 0) && (volume > regressionInput.minVolumeThreshold)) {	
			deltaValue = (avg - baseAvg) / baseAvg;
			state = getReliabilityState(regressionInput, deltaValue);					
		} else {
			deltaValue = 0;
			state = ReliabilityState.OK;
		}
		
		return Pair.of(state, deltaValue);
	}
	
	private String formatRateDelta(ReliabilityState state, boolean addParen, double value) {
		
		String result;
		
		if (state != ReliabilityState.OK) {
			
			StringBuilder builder = new StringBuilder(0);
			
			if (addParen) {
				builder.append(" (");
			}
			
			builder.append("+");
			builder.append(formatRate(value, false));

			if (addParen) {
				builder.append(")");
			}
			
			result = builder.toString();
		} else {
			result = "";
		}
		
		return result;
	}
	
	private Pair<Double, String> getAvgResponseState(double denom, double num, 
		double baseDenom, double baseNum, long volume, 
		RegressionInput regressionInput) {
		
		double avg;
		
		if (denom > 0) {
			avg = num / denom;
		} else {
			avg = 0;
		}
		
		double baseAvg;
		
		if (baseDenom > 0) {
			baseAvg = baseNum / baseDenom;
		} else {
			baseAvg = 0;
		}
		
		Pair<ReliabilityState, Double> statePair = getReliabilityState(baseAvg, avg, volume, regressionInput);
		String deltaStr = formatRateDelta(statePair.getFirst(), true, statePair.getSecond());
		
		return Pair.of(avg, deltaStr);
	}
	
	private ReportKeyFailures getAppFailureData(String serviceId, 
			ReliabilityReportInput input, Pair<DateTime, DateTime> timespan,
			ReportKeyResults reportKeyResult) {
		
		ReportKeyFailures result = new ReportKeyFailures();
		
		EventFilter failureFilter = getFailureTypeFilter(serviceId, timespan, input, reportKeyResult);
		
		if (failureFilter == null) {
			return result;
		}
		
		if (reportKeyResult.output.regressionData.regressionOutput.empty) {
			return result;
		}
		
		Map<String, EventResult> eventListMap = reportKeyResult.output.regressionData.regressionOutput.eventListMap;
			
		for (EventResult event : eventListMap.values()) {
				
			if (failureFilter.filter(event)) {
				continue;
			}
				
			result.failures += event.stats.hits;	
			result.eventCount++;			
		}
		
		Map<TransactionKey, TransactionData> transactionMap = reportKeyResult.output.transactionMap;
		
		if (transactionMap == null) {
			return result;	
		}
		
		Graph baseGraph = reportKeyResult.output.regressionData.regressionOutput.baseVolumeGraph;
		
		for (GraphPoint gp : baseGraph.points) {
			
			if (CollectionUtil.safeIsEmpty(gp.contributors)) {
				continue;
			}
			
			for (GraphPointContributor gpc : gp.contributors) {
				
				EventResult event = eventListMap.get(gpc.id);
				
				if ((event == null) || (failureFilter.filter(event))) {
					continue;
				}
				
				/* XXX removing bc of DB tx <-> event data inconsistency
				TransactionData eventTransaction = getEventTransactionData(transactionMap, event);
				
				if (eventTransaction == null) {
					continue;
				}
				*/
				
				result.baseFailures += gpc.stats.hits;						
			}
		}
		
		return result;
	}
		
	private ReliabilityState combineStates(ReliabilityState r1, ReliabilityState r2) {
		
		if ((r1 == ReliabilityState.Severe) || (r2 == ReliabilityState.Severe)) {
			return ReliabilityState.Severe;
		} else if ((r1 == ReliabilityState.Warning) || (r2 == ReliabilityState.Warning)) {
			return  ReliabilityState.Warning;
		}
		
		return ReliabilityState.OK;
	}
	
	private ReportKeyReliability getAppReliabilityData(String serviceId,
		ReportKeyResults reportKeyResult, ReliabilityReportInput input,
		ReportKeyTransactions transactionData, ReportKeyFailures failureData) {
		
		ReportKeyReliability result = new ReportKeyReliability();
							
		if (transactionData.transactionVolume > 0) {
			result.failRate = (double)failureData.failures / (double)transactionData.transactionVolume; 
		} else {
			result.failRate = 0;
		}
		
		double baseFailRate;
		
		if (transactionData.baseTransactions > 0) {
			baseFailRate = (double)failureData.baseFailures / (double)transactionData.baseTransactions; 
		} else {
			baseFailRate = 0;
		}
		
		RegressionInput regressionInput = reportKeyResult.output.regressionData.regressionOutput.regressionInput;
		
		Pair<ReliabilityState, Double> failurePair;
		
		if ((result.failRate > input.minFailRate) 
		&& (failureData.failures > regressionInput.minVolumeThreshold)) {
			failurePair = getReliabilityState(baseFailRate, result.failRate,
				transactionData.transactionVolume, regressionInput);

		} else {
			failurePair = Pair.of(ReliabilityState.OK, Double.valueOf(0));
		}
	
		result.failRateState = failurePair.getFirst();
		result.failureRateDelta = Math.max(0f, failurePair.getSecond());

		result.scoreState = getScoreState(input, reportKeyResult.score);
		result.reliabilityState = combineStates(result.failRateState , result.scoreState);
		
		result.reliabilityDesc = getReliabilityStateDesc(serviceId, result, 
			reportKeyResult, input, baseFailRate);
		
		result.failureRateDesc = getReliabilityFailRateDesc(result, reportKeyResult, 
			transactionData, failureData, baseFailRate);

		return result;
	}
	
	private String getReliabilityFailRateDesc(
			ReportKeyReliability reliabilityData,
			ReportKeyResults reportKeyResult,
			ReportKeyTransactions transactionData, ReportKeyFailures failureData,
			double baseFailRate) {
		
		StringBuilder failureDesc = new StringBuilder();
		
		failureDesc.append(formatLongValue(failureData.failures));
		failureDesc.append(" failures in ");
		failureDesc.append(formatLongValue(transactionData.transactionVolume));
		failureDesc.append(" calls (");
		failureDesc.append(formatRate(reliabilityData.failRate, true));
		failureDesc.append(") compared to ");
		failureDesc.append(formatLongValue(failureData.baseFailures));
		failureDesc.append(" in ");
		failureDesc.append(formatLongValue(transactionData.baseTransactions));
		failureDesc.append(" (");
		failureDesc.append(formatRate(baseFailRate, true));
		failureDesc.append(")");

		
		if (reportKeyResult.output.transactionRegInput != null) {
			failureDesc.append(" in the preceeding ");
			int baselineTimespan = reportKeyResult.output.transactionRegInput.baselineTimespan;
			failureDesc.append(prettyTime.formatDuration(TimeUtil.now().minusMinutes(baselineTimespan).toDate()));
			failureDesc.append(" baseline ");
		}
		
		return failureDesc.toString();
	}
	
	private String getReliabilityStateDesc(String serviceId,
		ReportKeyReliability reliabilityData,
		ReportKeyResults reportKeyResult, ReliabilityReportInput input,
		double baseFailRate) {
	
		StringBuilder failDelta = new StringBuilder();
		
		failDelta.append(formatRate(reliabilityData.failRate, true));
		
		if (reliabilityData.failRateState != ReliabilityState.OK) {	
			failDelta.append(" from ");
			failDelta.append(formatRate(baseFailRate, true));
		}
	
		StringBuilder relabilityDesc = new StringBuilder();
		
		relabilityDesc.append(reliabilityData.reliabilityState.toString().toUpperCase());
		relabilityDesc.append(" for ");
		relabilityDesc.append(reportKeyResult.output.reportKey.getFullName(serviceId, reportKeyResult));
		relabilityDesc.append(": ");

		if (reliabilityData.failRateState != ReliabilityState.OK) {
			relabilityDesc.append("Transaction fail rate ");
			relabilityDesc.append(failDelta);		
		} 
		
			
		if (reliabilityData.failRateState != ReliabilityState.OK) {
			relabilityDesc.append(". ");
		}
			
		relabilityDesc.append("Score = ");
		
		if ((int)reportKeyResult.score != reportKeyResult.score) {
			relabilityDesc.append(singleDigitFormatter.format(reportKeyResult.score));
		} else {
			relabilityDesc.append((int)reportKeyResult.score);
		}
		
		if (reliabilityData.reliabilityState != ReliabilityState.OK) {
			String timeRange = TimeUtil.getTimeRange(input.timeFilter);
			
			if (timeRange != null) {
				relabilityDesc.append( " in the last ");
				relabilityDesc.append(timeRange);
			}
		}
					
		return relabilityDesc.toString();
	}
		
	private ReportKeyTransactions getAppTransactionData(ReportKeyResults reportKeyResult) {
		
		ReportKeyTransactions result = new  ReportKeyTransactions();
		
		Map<TransactionKey, TransactionData> transactionMap = reportKeyResult.output.transactionMap;
		
		if (transactionMap == null) {
			return result;
		}
		
		RegressionInput regressionInput;
		
		if (reportKeyResult.output.transactionRegInput != null) {
			regressionInput = reportKeyResult.output.transactionRegInput;
		} else {
			regressionInput 	= reportKeyResult.output.regressionData.regressionOutput.regressionInput;
		}
		
		if (regressionInput == null) {
			return null;
		}

		double avgTimeNum = 0;
		double avgTimeDenom = 0;
		
		double baseAvgTimeNum = 0;
		double baseAvgTimeDenom = 0;
		
		for (TransactionData transactionData : transactionMap.values()) {
			
			result.errorVolume += transactionData.errorsHits;	
			result.transactionVolume += transactionData.stats.invocations;
					
			avgTimeNum += transactionData.stats.avg_time * transactionData.stats.invocations;
			avgTimeDenom += transactionData.stats.invocations;
			
			if (transactionData.baselineStats != null) {
				baseAvgTimeNum += transactionData.baselineStats.avg_time * transactionData.baselineStats.invocations;
				baseAvgTimeDenom += transactionData.baselineStats.invocations;
				result.baseTransactions += transactionData.baselineStats.invocations;
			}
		}
			
		Pair<Double, String> responsePair = getAvgResponseState(avgTimeDenom, avgTimeNum, 
				baseAvgTimeDenom, baseAvgTimeNum, result.transactionVolume, regressionInput);
		
		result.avgTimeDelta = responsePair.getFirst();
		result.deltaDesc = responsePair.getSecond();
		
		result.responseValue =  formatMilli(responsePair.getFirst()) + responsePair.getSecond();		
	
		return result;
	}
	
	private ReliabilityState getScoreState(ReliabilityReportInput input, double score) {
		
		if (input.scoreRanges == null) {
			return ReliabilityState.OK;
		}
		
		String[] parts = input.scoreRanges.split(ARRAY_SEPERATOR);
		
		if (parts.length != 2) {
			return ReliabilityState.OK;
		}
		
		int low = Integer.valueOf(parts[0]);
		int high = Integer.valueOf(parts[1]);
		
		if (score >= high) {
			return ReliabilityState.OK;
		}
		
		if (score < low) {
			return ReliabilityState.Severe;
		}
		
		return ReliabilityState.Warning;
	}
	
	private String getStatusPrefix(ReliabilityReportInput input, 
		ReliabilityState state) {
		
		if (input.statusPrefixes == null) {
			return null;
		}
		
		String[] parts = input.statusPrefixes.split(ARRAY_SEPERATOR);
		
		if (parts.length != 3) {
			return null;
		}
		
		String result = parts[state.ordinal()];	
		
		return result;
		
	}
	
	private String getAppStatusName(ReliabilityReportInput input, 
		ReliabilityState state, String name, String alertDesc) {
		
		String prefix = getStatusPrefix(input, state);
		
		if (prefix == null) {
			return "";
		}
		
		StringBuilder result = new StringBuilder();
		
		result.append(prefix);
		result.append(name);
		
		if ((input.alertNamePostfix != null) &&
			(alertDesc != null) && (!alertDesc.isEmpty())) {
			result.append(input.alertNamePostfix);
		}
		
		return result.toString();
		
	}
		
	private Map<String,ViewInfo> getViewMap(String serviceId) {
		
		Response<CategoriesResult> categoriesResponse = ApiCache.getCategories(apiClient, serviceId, true);
		
		if ((categoriesResponse.data == null) 
		|| (categoriesResponse.data.categories == null)) {
			return Collections.emptyMap();
		}
		
		Map<String,ViewInfo> result = new HashMap<String,ViewInfo>();
		
		for (com.takipi.api.client.data.category.Category category : 
			categoriesResponse.data.categories) {
			
			if (CollectionUtil.safeIsEmpty(category.views)) {
				continue;
			}
			
			for (ViewInfo viewInfo : category.views) {
				result.put(viewInfo.id, viewInfo);
			}
		}
		
		if (result.size() == 0) {
			
			Response<ViewsResult> viewsResponse = ApiCache.getViews(apiClient, serviceId);
			
			if ((viewsResponse.data != null) && (viewsResponse.data.views != null)) {
				for (SummarizedView summarizedView : viewsResponse.data.views) {
					result.put(summarizedView.id, null);
				}
			}
		}
		
		return result;
	}
	
	private ReportKeyAlerts getReportKeyAlerts(List<AlertSettings> AlertsSettings) {
	
		List<String> alertOnNewChannels = new ArrayList<String>();
		List<AlertSettings> alertOnAnomalies = new ArrayList<AlertSettings>();

		for (AlertSettings alertSettings : AlertsSettings) {
						
			if (alertSettings.alerts.alert_on_new) {
				
				if (!CollectionUtil.safeIsEmpty(alertSettings.alerts.channels)) {
					alertOnNewChannels.addAll(alertSettings.alerts.channels);	
				}	
				
				if (	alertSettings.alerts.channel_function != null) {
					alertOnNewChannels.add(alertSettings.alerts.channel_function.summary);	
				}
			}
			
			if ((alertSettings.alerts.anomaly_alert != null) 
			&& (alertSettings.alerts.anomaly_alert.alert_on_anomaly)
			&& (alertSettings.alerts.anomaly_alert.anomaly_function != null)) {
				alertOnAnomalies.add(alertSettings);
			}			
		}
		
		if ((alertOnNewChannels.size() == 0) && (alertOnAnomalies.size() == 0)) {
			return null;
		}
		
		ReportKeyAlerts result = new ReportKeyAlerts();
		
		result.hasNewAlert = alertOnNewChannels.size() > 0;
		result.hasAnomalyAlerts = alertOnAnomalies.size() > 0;

		StringBuilder desc = new StringBuilder();
	
		if (result.hasNewAlert) {
			desc.append("Alert on new events to: ");
			desc.append(String.join(", ", alertOnNewChannels).toLowerCase());
		}
		
		if (result.hasAnomalyAlerts) {
			
			if (result.hasNewAlert) {
				desc.append(". ");
			}
			
			desc.append("Alert on anomalies: ");

			for (AlertSettings anomalySettings : alertOnAnomalies) {
				
				String summary = anomalySettings.alerts.anomaly_alert.anomaly_function.summary;
				
				desc.append(summary);
				
				if (!CollectionUtil.safeIsEmpty(anomalySettings.alerts.channels)) {
					desc.append(" to ");
					desc.append(String.join(", ", anomalySettings.alerts.channels).toLowerCase());
				}
			}
		}
		
		result.description = desc.toString();
		
		return result;
	}
		
	private CategoryType getReportCategoryType(ReliabilityReportInput input) {
		
		switch (input.getReportMode()) {
			case Applications:
			case Apps_Extended:
				return CategoryType.app;
			case Tiers:
			case Tiers_Extended:
				return CategoryType.infra;
			default:
				return null;
		}
		
	}
	
	private void addAppToMap(Map<String, List<AlertSettings>> keysAlertsMaps,
		String app, AlertSettings alertSettings) {
		
		List<AlertSettings> appAlertSettings = keysAlertsMaps.get(app);
		
		if (appAlertSettings == null) {
			appAlertSettings = new ArrayList<AlertSettings>();
			keysAlertsMaps.put(app, appAlertSettings);
		}
		
		appAlertSettings.add(alertSettings);
	}
	
	private Map<String, ReportKeyAlerts> getReportKeyAlerts(
		ReliabilityReportInput input, String serviceId) {
			
		CategoryType categoryType = getReportCategoryType(input);
		
		if (categoryType == null) {
			return Collections.emptyMap();	
		}
		
		Map<String,ViewInfo> viewsMap = getViewMap(serviceId);
		
		if (CollectionUtil.safeIsEmpty(viewsMap)) {
			return Collections.emptyMap();
		}
		
		Response<AlertsSettingsResult> alertsResponse = ApiCache.getAlertsSettings(apiClient, serviceId);
		 
		if ((alertsResponse == null) || (alertsResponse.isBadResponse()) 
		|| (alertsResponse.data == null)) {
			return Collections.emptyMap();
		}
			
		Collection<String> appNames = new HashSet<String>(ApiCache.getApplicationNames(apiClient, serviceId, false));
		Collection<String> labelNames = new HashSet<String>(ApiCache.getLabelNames(apiClient, serviceId));

		Map<String, List<AlertSettings>> keysAlertsMaps = new HashMap<String, List<AlertSettings>>();
		
		List<AlertSettings> alerts = alertsResponse.data.alerts;
		  
		for (AlertSettings alertSettings : alerts) {
			
			if (alertSettings.alerts == null) {
				continue;
			}
			
			String viewKey = alertSettings.view_id;
			
			if (!viewsMap.containsKey(viewKey)) {
				continue;
			}
			
			ViewInfo viewInfo = viewsMap.get(viewKey);
			
			if ((viewInfo != null) && (viewInfo.filters != null)) {
				
				if (!CollectionUtil.safeIsEmpty(viewInfo.filters.apps)) {
					
					for (String app : viewInfo.filters.apps) {
						addAppToMap(keysAlertsMaps, app, alertSettings);			
					}
				}
				
				if (!CollectionUtil.safeIsEmpty(viewInfo.filters.labels)) {
					
					for (String label : viewInfo.filters.labels) {
						
						String tierName = InfraUtil.getTierNameFromLabel(label, categoryType);
						
						if (tierName == null) {
							continue;
						}
						
						String appKey;
						
						if (categoryType == CategoryType.app) {
							appKey = EnvironmentsFilterInput.toAppLabel(tierName);
						} else {
							appKey = tierName;
						}
											
						addAppToMap(keysAlertsMaps, appKey, alertSettings);			
					}
				}	
			} else {
				if (appNames.contains(alertSettings.view)) {
					addAppToMap(keysAlertsMaps, alertSettings.view, alertSettings);
				} else {
					String tierLabelName = InfraUtil.toTierLabelName(alertSettings.view, categoryType);
					
					if (labelNames.contains(tierLabelName)) {								
							
						String appKey;
							
						if (categoryType == CategoryType.app) {
							appKey = EnvironmentsFilterInput.toAppLabel(alertSettings.view);
						} else {
							appKey = alertSettings.view;
						}
							
						addAppToMap(keysAlertsMaps, appKey, alertSettings);										
					}
				}
			}
		}
				
		Map<String, ReportKeyAlerts> result = new HashMap<>();
		
		for (Map.Entry<String, List<AlertSettings>> entry : keysAlertsMaps.entrySet()) {
			
			ReportKeyAlerts reportKeyAlerts = getReportKeyAlerts(entry.getValue());
			
			if (reportKeyAlerts != null) {
				result.put(entry.getKey(), reportKeyAlerts);
			}
		}
		
		return result;
	}
		
	private void addExtendedFields(
			ReliabilityReportInput input, ReportRow row, 
			ReportKeyResults reportKeyResult, String serviceName,
			Map<String, ReportKeyAlerts> reportKeyAlertsMap) {
		
		ReportKeyAlerts reportKeyAlerts = reportKeyAlertsMap.get(reportKeyResult.output.reportKey.name);
		Pair<String, String> alertPair = getAlertStatus(input, reportKeyAlerts);

		String alertStatus = alertPair.getFirst();
		String alertDesc = alertPair.getSecond();

		String appStatusName = getAppStatusName(input, reportKeyResult.relability.reliabilityState, 
			serviceName, alertDesc);
		
		Object failRateDelta = getFailRateDelta(input, reportKeyResult.relability);
				
		row.set(ReliabilityReportInput.STATUS_NAME, appStatusName);

		row.set(ReliabilityReportInput.TRANSACTION_VOLUME, formatLongValue(reportKeyResult.transactions.transactionVolume));
		row.set(ReliabilityReportInput.TRANSACTION_AVG_RESPONSE, reportKeyResult.transactions.responseValue);
		
		row.set(ReliabilityReportInput.TRANSACTION_FAILURES, reportKeyResult.failures.failures);
		row.set(ReliabilityReportInput.TRANSACTION_FAIL_RATE, reportKeyResult.relability.failRate);
		row.set(ReliabilityReportInput.TRANSACTION_FAIL_RATE_DELTA, failRateDelta);
		row.set(ReliabilityReportInput.TRANSACTION_FAIL_DESC, reportKeyResult.relability.failureRateDesc);

		row.set(ReliabilityReportInput.ERROR_VOLUME, reportKeyResult.transactions.errorVolume);
		row.set(ReliabilityReportInput.ERROR_COUNT, reportKeyResult.failures.eventCount);
		
		row.set(ReliabilityReportInput.RELIABILITY_STATE, reportKeyResult.relability.reliabilityState.ordinal());	
		row.set(ReliabilityReportInput.RELIABILITY_DESC, reportKeyResult.relability.reliabilityDesc);	
		
		String alertDescValue;
		
		if ((alertDesc != null) && (!alertDesc.isEmpty())) {
			alertDescValue = alertDesc;
		} else {	
			alertDescValue = "Click to add alerts";
		}
		
		row.set(ReliabilityReportInput.ALERT_STATUS, alertStatus);	
		row.set(ReliabilityReportInput.ALERT_DESC, alertDescValue);	
	}
	
	public Pair<String, String> getAlertStatus(ReliabilityReportInput input,
		ReportKeyAlerts reportKeyAlerts) {
		
		if (input.alertStatusPrefixes == null) {
			return Pair.of("", "");
		}
		
		String[] parts = input.alertStatusPrefixes.split(ARRAY_SEPERATOR);
		
		if (parts.length != 3) {
			return Pair.of("", "");
		}
		
		StringBuilder status = new StringBuilder();
		String desc;
		
		if (reportKeyAlerts != null) {
			
			desc = reportKeyAlerts.description;
			
			if (reportKeyAlerts.hasNewAlert) {
				status.append(parts[1]);
			}
			
			if (reportKeyAlerts.hasAnomalyAlerts) {
				status.append(parts[2]);
			}
			
			if (status.length() == 0) {
				status.append(parts[0]);	
			}
		} else {
			desc = null;
			status.append(parts[0]);	
		}
		
		return Pair.of(status.toString(), desc);	
	}
	
	private void addTimelineFields(ReportRow row) {
		row.set(ReliabilityReportInput.TIMELINE_DIFF_STATE, "");
	}
	
	private void addDepCompareFields(ReportRow row, 
		ReportKeyResults reportKeyResult, Pair<Object, Object> fromTo) {
		

		String previousDepName = "";
		Integer previousDepState =  Integer.valueOf(0);
		Object previousDepFrom = fromTo.getFirst();
		
		if (reportKeyResult.output.reportKey instanceof DeploymentReportKey) {
			
			DeploymentReportKey depKey = (DeploymentReportKey)reportKeyResult.output.reportKey;
			
			if (depKey.compareToDep != null) {
				previousDepName = depKey.compareToDep.name;
				previousDepState = Integer.valueOf(1);
				
				if (depKey.compareToDep.first_seen != null) {
					DateTime firstSeen = TimeUtil.getDateTime(depKey.compareToDep.first_seen);
					
					long delta = DateTime.now().minus(firstSeen.getMillis()).getMillis();
					long timespan = TimeUnit.MILLISECONDS.toMinutes(delta) + TimeUnit.HOURS.toMinutes(1);
					
					Pair<Object, Object> timeFilter = getTimeFilterPair(null, 
						TimeUtil.getLastWindowMinTimeFilter((int)timespan));
		
					previousDepFrom = timeFilter.getFirst();
				}
			}
		} 
		
		row.set(ReliabilityReportInput.PREV_DEP_NAME, previousDepName);
		row.set(ReliabilityReportInput.PREV_DEP_FROM, previousDepFrom);
		row.set(ReliabilityReportInput.PREV_DEP_STATE, previousDepState);
	}
	
	private Pair<Pair<Object, Object>, String> getReportKeyTimeRange(
			EventsInput input, Pair<DateTime, DateTime> timeSpan, 
			ReportMode reportMode,RegressionWindow regressionWindow) {
		
		String timeRange;
		Pair<Object, Object> fromTo;
		
		boolean isTimeline = (reportMode == ReportMode.Timeline)
				  ||(reportMode == ReportMode.Timeline_Extended);
		
		if ((isTimeline) && (regressionWindow != null)) {
			DateTime activeStart = regressionWindow.activeWindowStart;
			DateTime activEnd = activeStart.plusMinutes(regressionWindow.activeTimespan);
			
			timeRange = null;
			fromTo = Pair.of(activeStart.getMillis(), activEnd.getMillis());
		} else {
			if (regressionWindow != null) {
				
				timeRange = TimeUtil.getTimeRange(regressionWindow.activeTimespan);
				
				DateTime from = regressionWindow.activeWindowStart;
				DateTime to = regressionWindow.activeWindowStart.plusMinutes(regressionWindow.activeTimespan);
				
				fromTo = getTimeFilterPair(Pair.of(from, to), 
					TimeUtil.getLastWindowMinTimeFilter(regressionWindow.activeTimespan));

			} else {
				fromTo = getTimeFilterPair(timeSpan, input.timeFilter);
				timeRange = TimeUtil.getTimeRange(input.timeFilter);	
			}
		}
		
		return Pair.of(fromTo, timeRange);	
	}
	
	private Map<String, String> getServiceNames() {
		
		Response<ServicesResult> response = ApiCache.getServices(apiClient);
		
		if ((response.isBadResponse()) || (response.data == null)
		|| (response.data.services == null)) {
			return Collections.emptyMap();
		}
		
		Map<String, String> result = new HashMap<String, String>();
		
		for (SummarizedService service: response.data.services) {
			result.put(service.id, EnvironmentsFunction.toServiceValue(service));
		}
		
		return result;
	}
	
	@Override
	protected List<List<Object>> processServiceEvents(Collection<String> serviceIds,
			String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan) {

		List<List<Object>> result = new ArrayList<List<Object>>();
		
		ReliabilityReportInput rrInput = (ReliabilityReportInput)input;
		
		Collection<ReportKeyOutput> reportKeyOutputs = executeReport(serviceId, rrInput, timeSpan);
		Collection<ReportKeyResults > reportKeyResults = getReportResults(serviceId, timeSpan, rrInput, reportKeyOutputs);
			
		ReportMode reportMode = rrInput.getReportMode();
		
		Map<String, ReportKeyAlerts> reportKeyAlertsMap = getReportKeyAlerts(rrInput, serviceId);
		
		 Map<String, String> serviceNames = getServiceNames();
		 
		for (ReportKeyResults reportKeyResult : reportKeyResults) {
			
			RegressionWindow regressionWindow = reportKeyResult.output.regressionData.regressionOutput.regressionWindow;
			
			Pair<Pair<Object, Object>, String> timeRangePair = getReportKeyTimeRange(rrInput, 
				timeSpan, reportMode, regressionWindow);
					
			Object newIssuesValue = formatIssues(rrInput, reportKeyResult.newIssues, reportKeyResult.severeNewIssues);
			Object regressionsValue = formatIssues(rrInput, reportKeyResult.regressions, reportKeyResult.criticalRegressions);
			Object slowdownsValue = formatIssues(rrInput, reportKeyResult.slowdowns, reportKeyResult.severeSlowdowns);
								
			String serviceValue = getServiceValue(reportKeyResult.output.reportKey.toString(), 
				serviceId, serviceIds);
			
			String name = reportKeyResult.output.reportKey.name;
				
			List<String> fields = getColumns(rrInput);
				
			ReportRow row = new ReportRow(fields);
			
			row.set(ViewInput.FROM, timeRangePair.getFirst().getFirst());
			row.set(ViewInput.TO, timeRangePair.getFirst().getSecond());
			row.set(ViewInput.TIME_RANGE, timeRangePair.getSecond());

			String serviceName = serviceNames.get(serviceId);
			
			if (serviceName == null) {
				serviceName = serviceId;
			}
				
			row.set(ReliabilityReportInput.SERVICE, serviceName);
			row.set(ReliabilityReportInput.KEY, name);
			row.set(ReliabilityReportInput.NAME, serviceValue);
			
			switch (rrInput.getReportMode()) {
				
				case Apps_Extended:
				case Timeline_Extended:
				case Tiers_Extended:
					addExtendedFields(rrInput, row, reportKeyResult, 
						serviceValue, reportKeyAlertsMap);	
					break;
				case Deployments:
					addDepCompareFields(row, reportKeyResult, timeRangePair.getFirst());
					break;
				case Timeline:
					addTimelineFields(row);
					break;
				default:
					break;	
			}
			
			row.set(ReliabilityReportInput.NEW_ISSUES, newIssuesValue);
			row.set(ReliabilityReportInput.REGRESSIONS, regressionsValue);
			row.set(ReliabilityReportInput.SLOWDOWNS, slowdownsValue);
			
			row.set(ReliabilityReportInput.NEW_ISSUES_DESC, reportKeyResult.newIssuesDesc);
			row.set(ReliabilityReportInput.REGRESSIONS_DESC, reportKeyResult.regressionsDesc);
			row.set(ReliabilityReportInput.SLOWDOWNS_DESC, reportKeyResult.slowDownsDesc);
			
			row.set(ReliabilityReportInput.SCORE, reportKeyResult.score);
			row.set( ReliabilityReportInput.SCORE_DESC, reportKeyResult.scoreDesc);
			
			result.add(row.values);
		}

		return result;
	}
	
	private String getPostfix(ReliabilityReportInput input, double value) {
		
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

		if (value < min) {
			return postfixes[0];
		}
		
		if (value < max) {
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
	
	private Object getReportKeyValue(Collection<String> serviceIds, String serviceId, Pair<DateTime, DateTime> timeSpan,
		ReliabilityReportInput input, ReportKeyResults reportKeyResult) {
			
		ReliabilityKpi graphType = ReliabilityReportInput.getKpi(input.graphType);

		if (graphType == null) {
			return reportKeyResult.score;
		}
		
		switch (graphType) {
			
			case NewErrors: return reportKeyResult.newIssues + reportKeyResult.severeNewIssues;
			case SevereNewErrors: return reportKeyResult.severeNewIssues; 
			case ErrorIncreases: return reportKeyResult.regressions + reportKeyResult.criticalRegressions;
			case SevereErrorIncreases: return reportKeyResult.criticalRegressions; 
			case Slowdowns: return reportKeyResult.slowdowns + reportKeyResult.severeSlowdowns;
			case SevereSlowdowns: return reportKeyResult.severeSlowdowns; 
			
			case ErrorVolume: 
				if (reportKeyResult.volumeData != null) {
					return reportKeyResult.volumeData.volume;
				} else {
					return 0;
				}
			
			case ErrorCount: 
				if (reportKeyResult.volumeData != null) {
					return reportKeyResult.volumeData.count;
				} else {
					return 0;
				}
			
			case ErrorRate: 
				if (reportKeyResult.volumeData != null) {
					return reportKeyResult.volumeData.rate;
				} else {
					return 0;
				}
			
			case FailRateDelta: 
				
				if (serviceIds.size() == 1) {
					return getFailRateDelta(serviceId, 	timeSpan, input, reportKeyResult, true);
				} else {
					return 0;
				}
			
			case FailRateDesc: return getFailRateDesc(serviceIds, serviceId, 
					timeSpan, input, reportKeyResult);
			
			case ScoreDesc: return reportKeyResult.scoreDesc; 
	
			case Score: return reportKeyResult.score; 
		}
		
		return 0;
	}
	
	private ReportKeyReliability getAppReliabilityData(String serviceId, 
			Pair<DateTime, DateTime> timespan, ReliabilityReportInput input, ReportKeyResults reportKeyResult) {
		
		ReportKeyFailures appFailureData = getAppFailureData(serviceId, input, timespan, reportKeyResult);
		ReportKeyTransactions appTransactionsData = getAppTransactionData(reportKeyResult);
		
		ReportKeyReliability result = getAppReliabilityData(serviceId, reportKeyResult, input, appTransactionsData, appFailureData);
				
		return result;
	}
	
	private Object getFailRateDesc(Collection<String> serviceIds, String serviceId, 
			Pair<DateTime, DateTime> timespan, ReliabilityReportInput input, ReportKeyResults reportKeyResult) {
			
		if (serviceIds.size() > 1) {
			return 0;
		}
			
		ReportKeyReliability appReliabilityData = getAppReliabilityData(serviceId, timespan, input, reportKeyResult);
		
		String result = appReliabilityData.failureRateDesc;
			
		return result;
	}
	
	private Object getFailRateDelta(String serviceId, Pair<DateTime, DateTime> timespan, 
			ReliabilityReportInput input, ReportKeyResults reportKeyResult, boolean postfix) {
		
		ReportKeyReliability appReliabilityData = getAppReliabilityData(serviceId, timespan, input, reportKeyResult);
		Object result = getFailRateDelta(input, appReliabilityData, true, postfix, true);
		
		return result;
	}
	
	private String getFailRateDelta(ReliabilityReportInput input, ReportKeyReliability appReliabilityData) {
		
		if (input.failRatePrefixes == null) {
			return "";
		}
		
		String[] parts = input.failRatePrefixes.split(ARRAY_SEPERATOR);
		
		if (parts.length != 3) {
			return "";
		}
				
		switch (appReliabilityData.failRateState) {
			case OK:
				return parts[0];
			case Warning:
				return parts[1];
			case Severe:
				return parts[2];
			default:
				throw new IllegalStateException(String.valueOf(appReliabilityData.failRateState));
			
		}
	}
	
	private Object getFailRateDelta(ReliabilityReportInput input, ReportKeyReliability appReliabilityData,
			boolean stringFormat, boolean postfix, boolean deltaPrefix) {
		
		if (appReliabilityData.failureRateDelta == 0) {
			return "";
		}
				
		Object result;
	
		String rateStr;
			
		if (appReliabilityData.failureRateDelta > 1) {
			if (deltaPrefix) {
				rateStr = "Δ>100%";	 	
			} else {
				rateStr = ">100%";	 
			}
		} else {		
			if (stringFormat) {
				rateStr = "+" + formatRate(appReliabilityData.failureRateDelta, true);
			} else {
				rateStr = null;
			}
		}
		
		if (rateStr != null) {
			
			if ((postfix) && (appReliabilityData.failRateState != ReliabilityState.OK)) {
				String statusPrefix = getStatusPrefix(input, appReliabilityData.failRateState);
				result = String.format("%s %s", rateStr, statusPrefix);
			} else {
				result = rateStr;
			}
			
		} else {
			result = appReliabilityData.failureRateDelta;
		}
		
		return result;
	}

	private List<Series> processGraph(ReliabilityReportInput input) {

		List<Series> result = new ArrayList<Series>();

		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		Collection<String> serviceIds = getServiceIds(input);

		for (String serviceId : serviceIds) {

			Collection<ReportKeyOutput> reportKeyOutputs = executeReport(serviceId, input, timeSpan);
			Collection<ReportKeyResults > reportKeyResults = getReportResults(serviceId, timeSpan, input, reportKeyOutputs);

			for (ReportKeyResults reportKeyResult : reportKeyResults) {

				Object keyValue = getReportKeyValue(serviceIds, serviceId, timeSpan, input, reportKeyResult);
				
				String seriesName;
				String postfix;
				
				if (keyValue instanceof Double) {
					postfix = getPostfix(input, (Double)keyValue);
				} else {
					postfix = null;
				}
				
				String name = reportKeyResult.output.reportKey.toString();
				
				if (postfix != null) {
					seriesName = getServiceValue(name + postfix, serviceId, serviceIds);
				} else {
					seriesName = getServiceValue(name, serviceId, serviceIds);
				}
								
				Series series = createGraphSeries(seriesName, 0);
				
				series.values
						.add(Arrays.asList(new Object[] { timeSpan.getSecond().getMillis(), keyValue }));

				result.add(series);
			}
		}

		return result;
	}
}
