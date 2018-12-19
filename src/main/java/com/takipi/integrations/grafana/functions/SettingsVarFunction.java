package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.SettingsVarInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GeneralSettings;
import com.takipi.integrations.grafana.settings.GrafanaSettings;


public class SettingsVarFunction extends EnvironmentVariableFunction
{	
	public static class Factory implements FunctionFactory
	{
		@Override
		public GrafanaFunction create(ApiClient apiClient)
		{
			return new SettingsVarFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass()
		{
			return SettingsVarInput.class;
		}
		
		@Override
		public String getName()
		{
			return "settingsVar";
		}
	}
	
	public SettingsVarFunction(ApiClient apiClient)
	{
		super(apiClient);
	}
	
	
	@Override
	protected void populateServiceValues(EnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {
		
		SettingsVarInput settingsVarInput = (SettingsVarInput)input;
		
		if (settingsVarInput.name == null) {
			return;
		}
				
		GeneralSettings settings = GrafanaSettings.getServiceSettings(apiClient, serviceId).getData().general;
		
		if (settings == null) {
			return;
		}
		
		Object value;
		
		try
		{
			Object rawValue = settings.getClass().getField(settingsVarInput.name).get(settings);
			
			if ((rawValue != null) && (settingsVarInput.convertToArray)) {
				 value = rawValue.toString().replaceAll(ARRAY_SEPERATOR, GRAFANA_SEPERATOR_RAW);
			} else {
				 value = rawValue;
			}
		}
		catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e)
		{
			throw new IllegalStateException(e);
		}
		
		if (value != null) {
			appender.append(value.toString());	
		}
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {
		
		SettingsVarInput settingsVarInput = (SettingsVarInput)input;
		Collection<String> serviceIds = getServiceIds(settingsVarInput);
		
		if (serviceIds.size() == 0) {
			appender.append(settingsVarInput.defaultValue);
		} else {
			super.populateValues(settingsVarInput, appender);
		}
	}

	
	@Override
	public  List<Series> process(FunctionInput functionInput) {
			
		if (!(functionInput instanceof SettingsVarInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
				
	}
	
}
