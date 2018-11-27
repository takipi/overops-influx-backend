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
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.result.event.EventSlimResult;
import com.takipi.api.client.result.event.EventsSlimVolumeResult;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.regression.RateRegression;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionReportInput;
import com.takipi.integrations.grafana.input.RegressionReportInput.ReportMode;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.RegressionReportSettings;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.DeploymentUtil;
import com.takipi.integrations.grafana.util.TimeUtil;

public class RegressionReportFunction extends RegressionFunction {
	
	private static final List<String> FIELDS = Arrays.asList(
			new String[] { "Name", "New Issues", "Regressions", "Slowdowns", "Score" });
	
	public static class Factory implements FunctionFactory {
		
		@Override
		public GrafanaFunction create(ApiClient apiClient)
		{
			return new RegressionReportFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass()
		{
			return RegressionReportInput.class;
		}
		
		@Override
		public String getName()
		{
			return "regressionReport";
		}
	}
	
	protected static class AsyncResult {
		RegressionOutput output;
		String key;
		
		protected AsyncResult( String key, RegressionOutput output) {
			this.output = output;
			this.key = key;
		}
	}

	public class RegressionAsyncTask extends BaseAsyncTask implements Callable<Object> {

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

				RegressionOutput output = runRegression(serviceId, input); 
				AsyncResult result = new AsyncResult(key, output);
				
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

	public RegressionReportFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private RegressionsInput getInput(RegressionReportInput reportInput, String name) {

		Gson gson = new Gson();
		String json = gson.toJson(reportInput);
		RegressionsInput result = gson.fromJson(json, RegressionsInput.class);

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

	private Collection<String> getRoutingVolumes(String serviceId, RegressionReportInput input,
			Pair<DateTime, DateTime> timeSpan) {
 		
		Collection<String> tiers = GrafanaSettings.getServiceSettings(apiClient, serviceId).getTierNames();
		
		List<String> result = new ArrayList<String>();

		if (tiers != null) {
			result.addAll(tiers);
		}
		
		if (result.size() >= input.limit) {
			return result;
		}

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
		
		result.addAll(limitVolumes(new ArrayList<VolumeOutput>(volumeMap.values()), 
			input.limit - result.size()));

		return result;
	}

	private List<VolumeOutput> getAppVolumes(String serviceId, RegressionReportInput input,
			Pair<DateTime, DateTime> timeSpan, Collection<String> apps) {

		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();

		for (String app : apps) {
			tasks.add(new AppVolumeAsyncTask(getInput(input, app), serviceId, app, timeSpan));
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
		protected double score;
		RateRegression regression;
		RegressionOutput regressionOutput;

		protected RegressionScore(RateRegression regression, String key,
				RegressionOutput regressionOutput,
				double score) {

			this.key = key;
			this.regression = regression;
			this.regressionOutput = regressionOutput;
			this.score = score;
		}
	}

	protected List<AsyncResult> processAsync(String serviceId, RegressionReportInput input,
			Pair<DateTime, DateTime> timeSpan, Collection<String> keys) {

		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
		List<AsyncResult> result = new ArrayList<AsyncResult>();

		for (String key : keys) {
			RegressionsInput keyInput = getInput(input, key);
			
			RegressionOutput regressionOutput = ApiCache.getRegressionOutput(apiClient, serviceId, keyInput, this, false);
			
			if (regressionOutput != null) {
				result.add(new AsyncResult(key, regressionOutput));
			} else {
				tasks.add(new RegressionAsyncTask(key, serviceId, keyInput, timeSpan));
			}			
		}
		
		if (tasks.size() == 1) {
			try {
				result.add((AsyncResult)(tasks.get(0).call()));
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		} else {
			List<Object> taskResults = executeTasks(tasks);
	
			for (Object taskResult : taskResults) {
	
				if (taskResult instanceof AsyncResult) {
					result.add((AsyncResult) taskResult);
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

	private Collection<String> getActiveDeployments(String serviceId, RegressionReportInput input) {

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
	
	

	private Collection<String> getActiveKeys(String serviceId, RegressionReportInput regInput,
			Pair<DateTime, DateTime> timeSpan) {
		
		Collection<String> keys;
		
		switch (regInput.mode) {
			
			case Deployments:
				keys = getActiveDeployments(serviceId, regInput);
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
		
		return keys;
	}
	
	protected List<RegressionScore> executeReport(String serviceId, RegressionReportInput regInput,
			Pair<DateTime, DateTime> timeSpan) {

		RegressionReportSettings regressionReportSettings = getRegressionReportSettings(serviceId);
		
		Collection<String> keys = getActiveKeys(serviceId, regInput, timeSpan);
		
		System.out.println("Executing report " + regInput.mode + " keys: " + Arrays.toString(keys.toArray()));

		List<RegressionScore> result = new ArrayList<RegressionScore>();

		List<AsyncResult> AsyncResults = processAsync(serviceId, regInput, timeSpan, keys);

		for (AsyncResult asyncResult : AsyncResults) {

			RegressionOutput regressionOutput = asyncResult.output;

			if ((regressionOutput == null) || (regressionOutput.rateRegression == null)) {
				continue;
			}
			
			double score = regressionOutput.getScore(regressionReportSettings);	
			
			RegressionScore regressionScore = new RegressionScore(regressionOutput.rateRegression, asyncResult.key, 
					regressionOutput, score);

			result.add(regressionScore);
		}

		if (ReportMode.Deployments.equals(regInput.mode)) {
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

		if (input.render == null) {
			throw new IllegalStateException("Missing render mode");
		}
		
		switch (input.render) {
			
			case Graph:
				return processGraph(input);
			case Grid:
				return super.process(functionInput);	
			default:
				throw new IllegalStateException("Illegal render mode " + input.render.toString());	
		}
	}

	@Override
	protected List<List<Object>> processServiceEvents(String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan) {

		List<List<Object>> result = new ArrayList<List<Object>>();
		List<RegressionScore> regressionScores = executeReport(serviceId, (RegressionReportInput) input, timeSpan);

		for (RegressionScore regressionScore : regressionScores) {			
			
			Object newIssues = formatIssueType(regressionScore.regressionOutput.newIssues, regressionScore.regressionOutput.severeNewIssues);
			Object regressions = formatIssueType(regressionScore.regressionOutput.regressions, regressionScore.regressionOutput.criticalRegressions);
			Object slowdowns = formatIssueType(regressionScore.regressionOutput.slowsdowns, regressionScore.regressionOutput.severeSlowsdowns);
			
			Object[] row = new Object[] { regressionScore.key, newIssues, regressions, slowdowns,
					regressionScore.score };

			result.add(Arrays.asList(row));
		}

		return result;
	}
	
	private String getPostfix(RegressionReportInput input, double score) {
		
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

	private List<Series> processGraph(RegressionReportInput input) {

		List<Series> result = new ArrayList<Series>();

		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
		Collection<String> serviceIds = getServiceIds(input);

		for (String serviceId : serviceIds) {

			List<RegressionScore> regressionScores = executeReport(serviceId, input, timeSpan);

			if (ReportMode.Deployments.equals(input.mode)) {
				sortDeployments(regressionScores, false);
			}

			for (RegressionScore regressionScore : regressionScores) {

				if (regressionScore.score == 0) {
					continue;
				}

				String seriesName;
				String postfix = getPostfix(input, regressionScore.score);
				
				if (postfix != null) {
					seriesName = regressionScore.key + postfix;
				} else {
					seriesName = regressionScore.key;
				}
				
				Series series = new Series();
				series.values = new ArrayList<List<Object>>();

				series.name = EMPTY_NAME;
				series.columns = Arrays.asList(new String[] { TIME_COLUMN, seriesName });
				series.values
						.add(Arrays.asList(new Object[] { timeSpan.getFirst().getMillis(), regressionScore.score }));

				result.add(series);
			}
		}

		return result;
	}
}
