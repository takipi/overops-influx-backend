package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EnvironmentsFilterInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;

public abstract class BaseServiceCompositeFunction extends BaseCompositeFunction
{
	public BaseServiceCompositeFunction(ApiClient apiClient)
	{
		super(apiClient);
	}

	protected abstract Collection<Pair<GrafanaFunction, FunctionInput>> getServiceFunctions(
		String serviceId, EnvironmentsFilterInput functionInput);
	
	@Override
	protected Collection<Pair<GrafanaFunction, FunctionInput>> getFunctions(FunctionInput functionInput)
	{
		EnvironmentsFilterInput input = (EnvironmentsFilterInput)functionInput;
		Collection<String> serviceIds = getServiceIds(input);
		
		Collection<Pair<GrafanaFunction, FunctionInput>> result = new ArrayList<Pair<GrafanaFunction, FunctionInput>>();
		
		for (String serviceId : serviceIds) {
			result.addAll(getServiceFunctions(serviceId, input));
		}
		
		return result;
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput)
	{
		if (!(functionInput instanceof EnvironmentsFilterInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		return super.process(functionInput);
	}
	
}
