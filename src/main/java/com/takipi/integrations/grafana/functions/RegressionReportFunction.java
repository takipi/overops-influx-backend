package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.api.client.util.regression.RateRegression;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionReportInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.output.Series;

public class RegressionReportFunction extends RegressionFunction {

	private static final List<String> FIELDS = Arrays.asList(new String[] { 
			"App Name","New Issues", "Severe New", "Regressions", "Severe Regressions", 
			"Score" });
	
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

	public RegressionReportFunction(ApiClient apiClient) {
		super(apiClient);
	}

	private RegressionsInput getInput(RegressionsInput input, String appName) {
		Gson gson = new Gson();
		String json = gson.toJson(input);
		RegressionsInput result = gson.fromJson(json, RegressionsInput.class);
		
		result.applications = appName;
		return result;
	}

	@Override
	protected List<String> getColumns(String fields) {
		return FIELDS;
	}
	
	@Override
	protected List<List<Object>> processServiceEvents(String serviceId, EventsInput input,
			Pair<DateTime, DateTime> timeSpan) {

		RegressionReportInput regInput = (RegressionReportInput) input;
		List<String> apps = ClientUtil.getApplications(apiClient, serviceId);

		if (apps == null) {
			return Collections.emptyList();
		}
		
		List<List<Object>> result = new ArrayList<List<Object>>();
		
		long delta = timeSpan.getSecond().minus(timeSpan.getFirst().getMillis()).getMillis();
		int factor = Math.max(1, (int)TimeUnit.MILLISECONDS.toDays(delta));
		
		for (String app : apps) {

			RegressionsInput appInput = getInput(regInput, app);
			RegressionOutput regressionOutput = executeRegerssion(serviceId, appInput, timeSpan);

			if (regressionOutput == null) {
				continue;
			}
			
			RateRegression rateRegression = regressionOutput.rateRegression;
			
			int severeIssues = rateRegression.getExceededNewEvents().size() + rateRegression.getSortedCriticalNewEvents().size();
			int newIssues = rateRegression.getAllNewEvents().size() - severeIssues;
			int criticalRegressions = rateRegression.getCriticalRegressions().size();
			int regressions =  rateRegression.getAllRegressions().size() - criticalRegressions;
			
			int newScore =  newIssues * regInput.newEventScore;
			int severeNewEventScore =  severeIssues * regInput.severeNewEventScore;
			int criticalRegressionsScore =  criticalRegressions * regInput.criticalRegressionScore;
			int regressionsScore =  regressions * regInput.regressionScore;
			
			int score = (newScore + severeNewEventScore + criticalRegressionsScore + regressionsScore) / factor;
			
			Object[] appData = new Object[] { app, 
				newIssues, severeIssues, regressions, criticalRegressions, score};
			
			result.add(Arrays.asList(appData));
		}
		
		return result;
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof RegressionReportInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		return super.process(functionInput);
	}
}
