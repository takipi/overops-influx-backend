package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.categories.Categories;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.api.client.util.regression.RateRegression;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionReportInput;
import com.takipi.integrations.grafana.input.RegressionReportInput.Mode;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.RegressionReportSettings;
import com.takipi.integrations.grafana.util.DeploymentUtil;
import com.takipi.integrations.grafana.util.TimeUtil;

public class RegressionReportFunction extends RegressionFunction {

	private static final List<String> FIELDS = Arrays.asList(
			new String[] { "App Name", "New Issues", "Severe New", "Regressions", "Severe Regressions", "Score" });

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new RegressionReportFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return RegressionReportInput.class;
		}

		@Override
		public String getName() {
			return "regressionReport";
		}
	}

	protected static class AsyncResult {
		RegressionOutput output;
		String key;
	}

	protected class RegressionAsyncTask extends BaseAsyncTask implements Callable<Object> {

		protected String key;
		protected String serviceId;
		protected RegressionsInput input;
		protected Pair<DateTime, DateTime> timeSpan;

		protected RegressionAsyncTask(String key, String serviceId, RegressionsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			this.key = key;
			this.serviceId = serviceId;
			this.input = input;
			this.timeSpan = timeSpan;
		}

		@Override
		public Object call() {

			beforeCall();

			try {

				RegressionOutput output = executeRegression(serviceId, input, timeSpan);
				AsyncResult result = new AsyncResult();

				result.output = output;
				result.key = key;

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

				Map<String, EventResult> appEventsMap = getEventMap(serviceId, input, timeSpan.getFirst(),
						timeSpan.getSecond(), VolumeType.hits, 0);

				long volume = 0;

				if (appEventsMap != null) {
					for (EventResult eventResult : appEventsMap.values()) {
						volume += eventResult.stats.hits;
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

	public RegressionReportFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private RegressionsInput getInput(RegressionReportInput reportInput, String name, Mode mode) {

		Gson gson = new Gson();
		String json = gson.toJson(reportInput);
		RegressionsInput result = (RegressionsInput) gson.fromJson(json, RegressionsInput.class);

		switch (reportInput.mode) {
		
			case Deployments:
				result.deployments = name;
				break;
				
			case Tiers:
				result.types = EventFilter.CATEGORY_PREFIX + name;
				break;
			
			default:
				result.applications = name;
				break;	
		}

		return result;
	}

	@Override
	protected List<String> getColumns(String fields) {
		return FIELDS;
	}

	private Pair<Long, Long> getGraphMinMaxEpochs(Graph graph) {

		if (graph.points == null) {
			return null;
		}

		long min = -1;
		long max = -1;

		for (GraphPoint gp : graph.points) {

			long epoch = TimeUtil.getLongTime(gp.time);

			if ((max == -1) || (max < epoch)) {
				max = epoch;
			}

			if ((min == -1) || (min > epoch)) {
				min = epoch;
			}
		}

		return Pair.of(Long.valueOf(min), Long.valueOf(max));
	}

	private Collection<String> getRoutingVolumes(String serviceId, RegressionReportInput input,
			Pair<DateTime, DateTime> timeSpan) {
 		
		Map<String, VolumeOutput> volumeMap = new HashMap<String, VolumeOutput>();
		
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timeSpan.getFirst(), 
			timeSpan.getSecond(), VolumeType.hits);
		
		if (eventsMap == null) {
			return Collections.emptyList();
		}
		
		Categories categories = GrafanaSettings.getServiceSettings(apiClient, serviceId).getCategories();
		
		for (EventResult event : eventsMap.values()) {
			
			if (event.error_origin == null) {
				continue;
			}
			
			Set<String> labels = categories.getCategories(event.error_origin.class_name);

			if (labels == null) {
				continue;
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

	private List<VolumeOutput> getAppVolumes(String serviceId, RegressionReportInput input,
			Pair<DateTime, DateTime> timeSpan, Collection<String> apps) {

		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();

		for (String app : apps) {
			tasks.add(new AppVolumeAsyncTask(getInput(input, app, Mode.Applications), serviceId, app, timeSpan));
		}

		List<VolumeOutput> result = new ArrayList<VolumeOutput>();
		List<Object> taskResults = executeTasks(tasks);

		for (Object taskResult : taskResults) {

			if (taskResult instanceof VolumeOutput) {
				result.add((VolumeOutput) taskResult);
			}
		}

		return result;
	}

	protected static class RegressionScore {

		protected String key;
		protected int newIssues;
		protected int severeNewEvent;
		protected int criticalRegressions;
		protected int regressions;
		protected double score;

		protected RegressionScore(String key, int newIssues, int severeNewEvent, int regressions,
				int criticalRegressions, double score) {

			this.key = key;
			this.newIssues = newIssues;
			this.severeNewEvent = severeNewEvent;
			this.criticalRegressions = criticalRegressions;
			this.regressions = regressions;
			this.score = score;
		}
	}

	@Override
	protected List<List<Object>> processServiceEvents(String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan) {

		List<List<Object>> result = new ArrayList<List<Object>>();
		List<RegressionScore> regressionScores = executeReport(serviceId, (RegressionReportInput) input, timeSpan);

		for (RegressionScore regressionScore : regressionScores) {

			Object[] row = new Object[] { regressionScore.key, regressionScore.newIssues,
					regressionScore.severeNewEvent, regressionScore.regressions, regressionScore.criticalRegressions,
					regressionScore.score };

			result.add(Arrays.asList(row));
		}

		return result;
	}

	protected List<AsyncResult> processAsync(String serviceId, RegressionReportInput input,
			Pair<DateTime, DateTime> timeSpan, Collection<String> keys) {

		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();

		for (String key : keys) {
			RegressionsInput keyInput = getInput(input, key, input.mode);
			tasks.add(new RegressionAsyncTask(key, serviceId, keyInput, timeSpan));
		}

		List<AsyncResult> result = new ArrayList<AsyncResult>();
		List<Object> taskResults = executeTasks(tasks);

		for (Object taskResult : taskResults) {

			if (taskResult instanceof AsyncResult) {
				result.add((AsyncResult) taskResult);
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

	private Collection<String> getActiveApplications(String serviceId, RegressionReportInput input,
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

	private Collection<String> getActiveDeployments(String serviceId, RegressionReportInput input,
			Pair<DateTime, DateTime> timeSpan) {

		List<String> activeDeps = ClientUtil.getDeployments(apiClient, serviceId, true);

		sortDeployments(activeDeps);

		List<String> result = new ArrayList<String>();

		for (int i = 0; i < Math.min(input.limit, activeDeps.size()); i++) {
			result.add(activeDeps.get(i));
		}

		int remainder = input.limit - result.size();

		if (remainder > 0) {
			List<String> nonActiveDeps = ClientUtil.getDeployments(apiClient, serviceId, false);
			sortDeployments(nonActiveDeps);

			for (int i = 0; i < Math.min(remainder, nonActiveDeps.size()); i++) {
				result.add(nonActiveDeps.get(i));
			}
		}

		return result;
	}

	protected List<RegressionScore> executeReport(String serviceId, RegressionReportInput regInput,
			Pair<DateTime, DateTime> timeSpan) {

		Collection<String> keys;

		switch (regInput.mode) {
			
			case Deployments:
				keys = getActiveDeployments(serviceId, regInput, timeSpan);
				break;
				
			case Tiers: 
				keys = getRoutingVolumes(serviceId, regInput, timeSpan);
				break;
			
			default:
				keys = getActiveApplications(serviceId, regInput, timeSpan);
				break;	
		}

		if (keys == null) {
			return Collections.emptyList();
		}

		System.out.println("Executing report " + regInput.mode + " keys: " + Arrays.toString(keys.toArray()));

		List<RegressionScore> result = new ArrayList<RegressionScore>();

		List<AsyncResult> AsyncResults = processAsync(serviceId, regInput, timeSpan, keys);

		for (AsyncResult asyncResult : AsyncResults) {

			RegressionOutput regressionOutput = asyncResult.output;

			if (regressionOutput == null) {
				continue;
			}
			
			RegressionReportSettings reportSettings = GrafanaSettings.getServiceSettings(apiClient, serviceId).regressionReport;

			if (reportSettings == null) {
				throw new IllegalStateException("Unable to acquire regression report settings for " + serviceId);
			}
			
			Pair<Long, Long> activeMinMax = getGraphMinMaxEpochs(regressionOutput.activeVolumeGraph);
			Pair<Long, Long> baselineMinMax = getGraphMinMaxEpochs(regressionOutput.baseVolumeGraph);

			double activeDelta = (activeMinMax.getSecond().doubleValue() - activeMinMax.getFirst().doubleValue());
			double baseDelta = (baselineMinMax.getSecond().doubleValue() - baselineMinMax.getFirst().doubleValue());

			double factor = activeDelta / baseDelta;

			RateRegression rateRegression = regressionOutput.rateRegression;

			int severeIssues = rateRegression.getExceededNewEvents().size()
					+ rateRegression.getSortedCriticalNewEvents().size();
			int newIssues = rateRegression.getAllNewEvents().size() - severeIssues;
			int criticalRegressions = rateRegression.getCriticalRegressions().size();
			int regressions = rateRegression.getAllRegressions().size() - criticalRegressions;

			int newEventsScore = newIssues * reportSettings.newEventScore;
			int severeNewEventScore = severeIssues * reportSettings.severeNewEventScore;
			int criticalRegressionsScore = criticalRegressions * reportSettings.criticalRegressionScore;
			int regressionsScore = regressions * reportSettings.regressionScore;

			double score = (double) (newEventsScore + severeNewEventScore + criticalRegressionsScore + regressionsScore)
					* factor;

			RegressionScore regressionScore = new RegressionScore(asyncResult.key, newIssues, severeIssues, regressions,
					criticalRegressions, score);

			result.add(regressionScore);
		}

		if (Mode.Deployments.equals(regInput.mode)) {
			sortDeployments(result, true);
		}

		return result;
	}

	private void sortDeployments(List<RegressionScore> scores, boolean asc) {
		scores.sort(new Comparator<RegressionScore>() {

			@Override
			public int compare(RegressionScore o1, RegressionScore o2) {

				if (asc) {
					return DeploymentUtil.compareDeployments(o1.key, o2.key);
				} else {
					return DeploymentUtil.compareDeployments(o2.key, o1.key);
				}
			}
		});
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof RegressionReportInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		RegressionReportInput input = (RegressionReportInput) functionInput;

		if (input.graph) {
			return processGraph(input);
		} else {
			return super.process(functionInput);
		}
	}

	private List<Series> processGraph(RegressionReportInput input) {

		List<Series> result = new ArrayList<Series>();

		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		Collection<String> serviceIds = getServiceIds(input);

		for (String serviceId : serviceIds) {

			List<RegressionScore> regressionScores = executeReport(serviceId, input, timeSpan);

			if (Mode.Deployments.equals(input.mode)) {
				sortDeployments(regressionScores, false);
			}

			for (RegressionScore regressionScore : regressionScores) {

				if (regressionScore.score == 0) {
					continue;
				}

				Series series = new Series();
				series.values = new ArrayList<List<Object>>();

				series.name = EMPTY_NAME;
				series.columns = Arrays.asList(new String[] { TIME_COLUMN, regressionScore.key });
				series.values
						.add(Arrays.asList(new Object[] { timeSpan.getFirst().getMillis(), regressionScore.score }));

				result.add(series);
			}
		}

		return result;
	}
}
