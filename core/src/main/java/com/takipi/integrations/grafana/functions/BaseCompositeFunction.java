package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;

public abstract class BaseCompositeFunction extends GrafanaFunction {
	protected class FunctionResult {
		protected GrafanaFunction function;
		protected FunctionInput input;
		protected List<Series> series;
	}
	
	protected class FunctionTask implements Callable<Object> {

		protected GrafanaFunction function;
		protected FunctionInput input;
		
		protected FunctionTask(GrafanaFunction function, FunctionInput input) {
			this.function = function;
			this.input = input;
		}
		
		@Override
		public Object call() throws Exception {
			FunctionResult result = new FunctionResult();
			
			result.function = function;
			result.input = input;
			result.series = function.process(input);
			
			return result;
		}
		
	}
	
	public BaseCompositeFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	protected abstract Collection<Pair<GrafanaFunction, FunctionInput>> getFunctions(FunctionInput functionInput);

	@Override
	public List<Series> process(FunctionInput functionInput) {
		Collection<Pair<GrafanaFunction, FunctionInput>> functions = getFunctions(functionInput);
		
		if (functions.isEmpty()) {
			return Collections.emptyList();
		}
		
		List<Series> result;
		List<Callable<Object>> tasks = new ArrayList< Callable<Object>>();

		if (functions.size() == 1) {
			Pair<GrafanaFunction, FunctionInput> pair = functions.iterator().next();
			result = pair.getFirst().process(pair.getSecond());
		} else {
			result = new ArrayList<Series>(tasks.size());
			
			for (Pair<GrafanaFunction, FunctionInput> pair : functions) {
				tasks.add(new FunctionTask(pair.getFirst(), pair.getSecond()));
			}
			
			List<Object> taskResults = executeTasks(tasks, true);
			Map<FunctionInput, List<Series>> results = new HashMap<FunctionInput, List<Series>>();			
		
			for (Object taskResult : taskResults) {
				
				if (!(taskResult instanceof FunctionResult)) {
					continue;
				}
				
				FunctionResult functionResult = (FunctionResult)taskResult;
				results.put(functionResult.input, functionResult.series);
			}
			
			for (Pair<GrafanaFunction, FunctionInput> funcPair : functions) {
				List<Series> funcResult = results.get(funcPair.getSecond());
				
				if (funcResult !=  null) {
					result.addAll(funcResult);
				}
			}
		}
			
		return result;
	}
}
