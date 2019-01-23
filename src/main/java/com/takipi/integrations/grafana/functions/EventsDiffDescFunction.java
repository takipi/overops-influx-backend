package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.List;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.common.util.CollectionUtil;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.EventsDiffInput;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;

public class EventsDiffDescFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory
	{
		@Override
		public GrafanaFunction create(ApiClient apiClient)
		{
			return new EventsDiffDescFunction(apiClient);
		}
		
		@Override
		public Class<?> getInputClass()
		{
			return EventsDiffInput.class;
		}
		
		@Override
		public String getName()
		{
			return "eventsDiffDesc";
		}
	}
	
	
	public EventsDiffDescFunction(ApiClient apiClient)
		{
		super(apiClient);
	}
	
	
	@Override
	public List<Series> process(FunctionInput functionInput)
	{
		if (!(functionInput instanceof EventsDiffInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
	}
	
	private void addPlural(StringBuilder value, Collection<String> col) {
		if (col.size() > 1) {
			value.append("s");
		}
		
		value.append(" ");
	}

	private void addGroup(StringBuilder value, Collection<String> apps, 
		Collection<String> deps, Collection<String> srvs, String timeDiff) {
		
		value.append("(");
		
		if ((CollectionUtil.safeIsEmpty(apps)) && (CollectionUtil.safeIsEmpty(deps)) 
			&& (CollectionUtil.safeIsEmpty(srvs))) {
			value.append("All events");
		} else {
			
			boolean comma = false;
			
			if (!CollectionUtil.safeIsEmpty(apps)) {
				value.append("App");
				addPlural(value, apps);
				value.append(String.join(", ", apps));
				comma = true;
			}
					
			if (!CollectionUtil.safeIsEmpty(deps)) {
				
				if (comma) {
					value.append(", ");	
				}
				
				value.append("Deployment");
				addPlural(value, deps);
				value.append(String.join(", ", deps));
				
				comma = true;
			}
					
			if (!CollectionUtil.safeIsEmpty(srvs)) {
				
				if (comma) {
					value.append(", ");	
				}
				
				value.append("Server");
				addPlural(value, srvs);
				value.append(String.join(", ", srvs));
			}
		}
		
		if ((timeDiff != null) && (!timeDiff.equals(GrafanaFunction.NONE))) {
			value.append(" ");
			value.append(timeDiff);
			value.append(" ago");
		}
		
		value.append(")");
	}
	
	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender)
	{
		
		EventsDiffInput edInput = (EventsDiffInput)input;
		
		EventsDiffInput eventsDiffInput = (EventsDiffInput)input;
		
		String json = new Gson().toJson(input);
		EventsInput targetInput =  new Gson().fromJson(json, edInput.getClass());
				
		targetInput.applications = eventsDiffInput.compareToApplications;
		targetInput.servers = eventsDiffInput.compareToServers;
		targetInput.deployments = eventsDiffInput.compareToDeployments;
		
		Collection<String> appsA = edInput.getApplications(apiClient, serviceId);
		Collection<String> appsB = targetInput.getApplications(apiClient, serviceId);
		
		Collection<String> depsA = edInput.getDeployments(serviceId);
		Collection<String> depsB = targetInput.getDeployments(serviceId);
		
		Collection<String> srvA = edInput.getServers(serviceId);
		Collection<String> srvB = targetInput.getServers(serviceId);
		
		boolean hadTimeDiff = (edInput.timeDiff != null) || (!edInput.timeDiff.equals(NONE));
		
		if ((Objects.equal(appsA, appsB)) 
		&& (Objects.equal(depsA, depsB)) 
		&& (Objects.equal(srvA, srvB))
		&& (!hadTimeDiff)) { 
			appender.append("Select filters from group A to compare with group B");
		}
		
		StringBuilder result = new StringBuilder();
		
		addGroup(result, appsB, depsB, srvB, null);

		result.append(" compared to ");

		addGroup(result, appsA, depsA, srvA, edInput.timeDiff);
		
		appender.append(result.toString());
	}
}