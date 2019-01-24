package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.List;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.common.util.CollectionUtil;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.EventsDiffDescInput;
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
			return EventsDiffDescInput.class;
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
		if (!(functionInput instanceof EventsDiffDescInput)) {
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
		Collection<String> deps, Collection<String> srvs, 
		String timeDiff, boolean enclose) {
		
		if (enclose) {
			value.append("(");
		}
		
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
		
		if ((timeDiff != null) && (!timeDiff.equals(EventsDiffFunction.NO_DIFF))) {
			value.append(" ");
			value.append(timeDiff);
			value.append(" ago");
		}
		
		if (enclose) {
			value.append(")");
		}
	}
	
	private String getFilterBDesc(EventsDiffInput input, String serviceId) {
		
		StringBuilder result = new StringBuilder();
		
		String json = new Gson().toJson(input);
		EventsInput targetInput =  new Gson().fromJson(json, input.getClass());
				
		targetInput.applications = input.compareToApplications;
		targetInput.servers = input.compareToServers;
		targetInput.deployments = input.compareToDeployments;
		
		Collection<String> appsB = targetInput.getApplications(apiClient, serviceId);
		Collection<String> depsB = targetInput.getDeployments(serviceId);
		Collection<String> srvB = targetInput.getServers(serviceId);
		
		addGroup(result, appsB, depsB, srvB, null, false);
		
		return result.toString();
	}
	
	private String getFilterADesc(EventsDiffInput input, String serviceId) {
		
		StringBuilder result = new StringBuilder();
						
		Collection<String> appsA = input.getApplications(apiClient, serviceId);
		Collection<String> depsA = input.getDeployments(serviceId);
		Collection<String> srvA = input.getServers(serviceId);
		
		addGroup(result, appsA, depsA, srvA, input.timeDiff, false);
		
		return result.toString();
	}
	
	private String getCombinedDesc(EventsDiffInput input, String serviceId) {
				
		String json = new Gson().toJson(input);
		EventsInput targetInput =  new Gson().fromJson(json, input.getClass());
				
		targetInput.applications = input.compareToApplications;
		targetInput.servers = input.compareToServers;
		targetInput.deployments = input.compareToDeployments;
		
		Collection<String> appsA = input.getApplications(apiClient, serviceId);
		Collection<String> appsB = targetInput.getApplications(apiClient, serviceId);
		
		Collection<String> depsA = input.getDeployments(serviceId);
		Collection<String> depsB = targetInput.getDeployments(serviceId);
		
		Collection<String> srvA = input.getServers(serviceId);
		Collection<String> srvB = targetInput.getServers(serviceId);
		
		boolean hadTimeDiff = (input.timeDiff != null) 
		|| (!input.timeDiff.equals(EventsDiffFunction.NO_DIFF));
		
		if ((Objects.equal(appsA, appsB)) 
		&& (Objects.equal(depsA, depsB)) 
		&& (Objects.equal(srvA, srvB))
		&& (!hadTimeDiff)) { 
			return "Select filters from group A to compare with group B";
		}
		
		StringBuilder result = new StringBuilder();
		
		addGroup(result, appsB, depsB, srvB, null, true);

		result.append(" vs. ");

		addGroup(result, appsA, depsA, srvA, input.timeDiff, true);
		
		return result.toString();
	}
	
	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender)
	{
		
		EventsDiffDescInput edInput = (EventsDiffDescInput)input;
		
		switch (edInput.getDescType()) {
			
			case Combined: {
				appender.append(getCombinedDesc(edInput, serviceId));
				break;
			}
			
			case FilterA: {
				appender.append(getFilterADesc(edInput, serviceId));
				break;
			}
			
			case FilterB: {
				appender.append(getFilterBDesc(edInput, serviceId));
				break;
			}
		}
	}
}
