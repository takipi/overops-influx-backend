package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.LimitVariableInput;
import com.takipi.integrations.grafana.output.Series;

public class LimitVariableFunction extends VariableFunction {
	
	private static final String prefix = "var-%s=";
	private static final String postfix = "&";

	
	public static class Factory implements FunctionFactory {
		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new LimitVariableFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass() {
			return LimitVariableInput.class;
		}
		
		@Override
		public String getName()	{
			return "limitVariable";
		}
	}
	
	public LimitVariableFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {
		
		LimitVariableInput pvInput = (LimitVariableInput)input;
		
		String value;  
		Collection<String> values = pvInput.getValues();
		
		if (pvInput.name == null) {
					
			if (values.size() > 0) {		
				value = values.iterator().next();
			} else {
				value = GrafanaFunction.ALL;
			}
			
			appender.append(value);
		} else {
			
			String varPrefix = String.format(prefix, pvInput.name);
			String joined = String.join(postfix + varPrefix, values);
			
			if (values.size() > 0) {
				value = varPrefix + joined;
			} else {
				value = "";
			}
		}
		
		appender.append(value);

		
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof LimitVariableInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
	}
}
