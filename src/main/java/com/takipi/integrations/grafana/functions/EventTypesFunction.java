package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.EventTypesInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.udf.infra.Categories;

public class EventTypesFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new EventTypesFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EventTypesInput.class;
		}

		@Override
		public String getName() {
			return "eventTypes";
		}
	}

	public EventTypesFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender) {

		if (!(input instanceof ViewInput)) {
			throw new IllegalArgumentException("input");
		}

		EventTypesInput eventInput = (EventTypesInput) input;

		for (String type : eventInput.getTypes()) {
			appender.append(type);
		}

		super.populateValues(input, appender);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, String[] serviceIds, String serviceId,
			VariableAppender appender) {

		EventTypesInput eventInput = (EventTypesInput) input;

		String viewId = getViewId(serviceId, eventInput.view);

		if (viewId == null) {
			return;
		}

		Collection<EventResult> events = getEventList(serviceId, viewId, eventInput, DateTime.now().minusDays(14),
				DateTime.now());

		if (events == null) {
			return;
		}
		
		Set<String> exceptionTypes = new TreeSet<String>();
		Set<String> labelTypes = new TreeSet<String>();

		for (EventResult event : events) {
			
			if (event.name != null) {
				exceptionTypes.add(event.name);
			}
			
			if (event.error_origin != null) {
			
				Set<String> labels = Categories.defaultCategories()
						.getCategories(event.error_origin.class_name);
				
				if (labels != null) {
					labelTypes.addAll(labels);
				}
			}
		}
		
		for (String label : labelTypes) {
			appender.append(EventFilter.CATEGORY_PREFIX + label);
		}

		for (String exceptionType : exceptionTypes) {
			appender.append(EventFilter.TYPE_PREFIX + exceptionType);
		}
	}
}
