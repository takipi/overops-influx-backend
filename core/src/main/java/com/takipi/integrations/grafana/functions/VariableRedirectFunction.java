package com.takipi.integrations.grafana.functions;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.VariableRedirectInput;
import com.takipi.integrations.grafana.output.Series;

public class VariableRedirectFunction extends VariableFunction {
	private static final String DICTIONARY_SEPERATOR = Pattern.quote(":");
	
	public static class Factory implements FunctionFactory {
		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new VariableRedirectFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass() {
			return VariableRedirectInput.class;
		}
		
		@Override
		public String getName() {
			return "variableRedirect";
		}
	}
	
	public VariableRedirectFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	private Map<String, String> getDictionaryMap(String dictionary) {
		
		Map<String, String> result = new HashMap<String, String>();
		
		if (dictionary == null) {
			return Collections.emptyMap();
		}
		
		String [] parts = dictionary.split(ARRAY_SEPERATOR);
		
		for (String part : parts) {
		
			String[] keyValue = part.split(DICTIONARY_SEPERATOR);
			
			if (keyValue.length != 2) {
				continue;
			}
			
			result.put(keyValue[0], keyValue[1]);
		}
		
		return result;
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {
		VariableRedirectInput varReInput = (VariableRedirectInput)input;
		
		if (varReInput.variable == null) {
			return;
		}
		
		Map<String, String> dictionary = getDictionaryMap(varReInput.dictionary); 
		String varName = dictionary.get(varReInput.variable);
		
		if (varName != null) {
			appender.append(varName);
		} else {
			System.err.println("Could not redirect " +  varReInput.variable + " from " + varReInput.dictionary);
		}	
	}
	
	@Override
	public  List<Series> process(FunctionInput functionInput) {
			
		if (!(functionInput instanceof VariableRedirectInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
				
	}
	
}
