package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.Stats;
import com.takipi.api.client.data.transaction.TransactionGraph;
import com.takipi.api.client.data.transaction.TransactionGraph.GraphPoint;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionAvgGraphInput;
import com.takipi.integrations.grafana.input.TransactionsGraphInput;
import com.takipi.integrations.grafana.output.Series;

public class TransactionAvgGraphFunction extends TransactionsGraphFunction
{

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionAvgGraphFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionAvgGraphInput.class;
		}

		@Override
		public String getName() {
			return "transactionsAvgGraph";
		}
	}
	
	public TransactionAvgGraphFunction(ApiClient apiClient)
	{
		super(apiClient);
	}
	
	
	@Override
	protected Collection<TransactionGraph> getTransactionsGraphs(String serviceId, 
			String viewId, Pair<DateTime, DateTime> timeSpan, TransactionsGraphInput
			input, int pointsWanted) {
				
		Collection<TransactionGraph> transactionGraphs = super.getTransactionsGraphs(serviceId, viewId, timeSpan, input, pointsWanted);
		
		if (transactionGraphs == null) {
			return Collections.emptyList();
		}
			
		List<TransactionGraph> result = new ArrayList<TransactionGraph>();
		
		for (TransactionGraph transactionGraph : transactionGraphs) {
				
			if (transactionGraph.points.size() == 0) {
				continue;
			}
			
			double value = 0;
			long sum = 0;
			
			for (GraphPoint gp : transactionGraph.points) {
				value += gp.stats.avg_time * gp.stats.invocations;
				sum += gp.stats.invocations;
			}
			
			double avg;
			
			if (sum > 0) {
				avg = value / sum;
			} else {
				avg = 0;
			}
			
			TransactionGraph avgGraph = new TransactionGraph();
			
			avgGraph.name = transactionGraph.name;
			avgGraph.class_name = transactionGraph.class_name;
			avgGraph.method_name = transactionGraph.method_name;
			avgGraph.method_desc = transactionGraph.method_desc;
										
			Stats stats = new Stats();
			
			stats = new Stats();
			stats.avg_time = avg;
			stats.invocations = sum;
			
			GraphPoint p1 = new GraphPoint();
			p1.time = transactionGraph.points.get(0).time;
			p1.stats = stats;

			GraphPoint p2 = new GraphPoint();
			p2.time = transactionGraph.points.get(transactionGraph.points.size() - 1).time;
			p2.stats = stats;
			
			avgGraph.points = Arrays.asList(new GraphPoint[] {p1, p2});
			result.add(avgGraph);
		}
		
		return result;
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof TransactionAvgGraphInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		return super.process(functionInput);
	}
	
}
