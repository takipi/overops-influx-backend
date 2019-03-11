package com.takipi.integrations.grafana.functions;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.ConvertToArrayInput;
import com.takipi.integrations.grafana.input.FunctionInput;

public class ConvertToArrayFunction extends VariableFunction
{
	
	public ConvertToArrayFunction(ApiClient apiClient)
	{
		super(apiClient);
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new ConvertToArrayFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return ConvertToArrayInput.class;
		}

		@Override
		public String getName() {
			return "convertToArray";
		}
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {
		ConvertToArrayInput caInput = (ConvertToArrayInput)input;
		
		if ((caInput.array != null) && (caInput.prefix != null)) {
			
			String[] parts = caInput.array.split(GRAFANA_SEPERATOR);
			StringBuilder value = new StringBuilder();
			
			for (String part : parts) {			
				value.append(caInput.prefix);
				value.append(part);
				value.append(caInput.postfix);
			}
						
			appender.append(value.toString());
		}
		
	}
}

