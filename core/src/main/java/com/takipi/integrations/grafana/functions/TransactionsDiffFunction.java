package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.List;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseEventVolumeInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsDiffInput;
import com.takipi.integrations.grafana.output.Series;

public class TransactionsDiffFunction extends TransactionsListFunction
{
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsDiffFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionsDiffInput.class;
		}

		@Override
		public String getName() {
			return "transactionsDiff";
		}
	}

	
	@Override
	protected Collection<TransactionGraph> getBaselineTransactionGraphs(String serviceId, String viewId,
			BaseEventVolumeInput input, Pair<DateTime, DateTime> timeSpan,
			RegressionInput regressionInput, RegressionWindow regressionWindow)
	{
		TransactionsDiffInput tdInput = (TransactionsDiffInput)input;
		
		Gson gson = new Gson();
		String json = gson.toJson(input);
		BaseEventVolumeInput baselineInput = gson.fromJson(json, input.getClass());
		
		baselineInput.applications = tdInput.baselineApplications;
		baselineInput.deployments = tdInput.deployments;
		baselineInput.servers = tdInput.servers;
		
		Collection<TransactionGraph> result = getTransactionGraphs(baselineInput, serviceId, 
			viewId, timeSpan, null, baselineInput.pointsWanted,  0, 0);
		
		return result;
	}

	public TransactionsDiffFunction(ApiClient apiClient)
	{
		super(apiClient);
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput)
	{
		if (!(functionInput instanceof TransactionsDiffInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
	}
	
}
