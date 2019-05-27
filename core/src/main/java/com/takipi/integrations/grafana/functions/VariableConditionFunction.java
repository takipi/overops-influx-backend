package com.takipi.integrations.grafana.functions;

import java.util.Objects;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.VariableConditionInput;

public class VariableConditionFunction extends VariableFunction{

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new VariableConditionFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return VariableConditionInput.class;
		}

		@Override
		public String getName() {
			return "condition";
		}
	}
	
	public VariableConditionFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender)
	{
		VariableConditionInput vcInput = (VariableConditionInput)input;
		
		String value;
		
		if (Objects.equals(vcInput.value, vcInput.compareTo)) {
			value = vcInput.trueValue;
		} else {
			value = vcInput.falseValue;
		}
		
		appender.append(value);	
	}
	
}
 