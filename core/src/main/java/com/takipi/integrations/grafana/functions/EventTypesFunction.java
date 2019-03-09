package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.EventTypesInput;
import com.takipi.integrations.grafana.input.EventTypesInput.EventTypes;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings;
import com.takipi.integrations.grafana.settings.input.GeneralSettings;
import com.takipi.integrations.grafana.util.TimeUtil;

public class EventTypesFunction extends EnvironmentVariableFunction {

	private static int DEFAULT_TIME_DAYS = 7;
	
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

		if (!(input instanceof EventTypesInput)) {
			throw new IllegalArgumentException("input");
		}

		super.populateValues(input, appender);
	}
	
	private EventTypesInput getEventTypesInput(BaseEnvironmentsInput input) {
		
		EventTypesInput eventInput = (EventTypesInput) input;

		Gson gson = new Gson();
		String json = gson.toJson(eventInput);
		
		EventTypesInput result = gson.fromJson(json, eventInput.getClass());
		
		if (eventInput.timeFilter != null) {
			Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(eventInput.timeFilter);
			result.timeFilter = TimeUtil.getTimeFilter(timespan);	
		} else {			
			result.timeFilter  = TimeUtil.getLastWindowTimeFilter(TimeUnit.DAYS.toMillis(DEFAULT_TIME_DAYS));
		}
		
		return result;
		
	}

	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {

		EventTypesInput evInput = (EventTypesInput) input;
		EventTypesInput eventInput = getEventTypesInput(evInput);
				
		String viewId = getViewId(serviceId, eventInput.view);

		if (viewId == null) {
			return;
		}
		
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(eventInput.timeFilter);

		
		Map<String, EventResult> events = getEventMap(serviceId, eventInput,
				timespan.getFirst(), timespan.getSecond(), null);
				
		if (events == null) {
			return;
		}
		
		Collection<String> types;
		
		if (eventInput.types != null) {
			types = eventInput.getTypes();
		} else {
			GeneralSettings settings = GrafanaSettings.getData(apiClient, serviceId).general;
			
			if (settings != null) {
				types = settings.getDefaultTypes();
			} else {
				types = Collections.emptyList();
			}
		}
			
		Set<String> eventTypes = new HashSet<String>();
		Set<String> exceptionTypes = new TreeSet<String>();
		Set<String> categoryNames = new TreeSet<String>();
			
		Categories categories = GrafanaSettings.getServiceSettings(apiClient, serviceId).getCategories();

		for (EventResult event : events.values()) {
			
			DateTime firstSeen = dateTimeFormatter.parseDateTime(event.first_seen);
			
			if ((eventInput.newOnly) && (!firstSeen.isAfter(timespan.getFirst()))) {
				continue;
			}
			
			eventTypes.add(event.type);
			
			if ((event.name != null) && (!types.contains(event.name))) {
				exceptionTypes.add(event.name);
			}
			
			if (categories != null) {
			
				if (event.error_origin != null) {
					Set<String> originLabels = categories.getCategories(event.error_origin.class_name);
					
					if (!CollectionUtil.safeIsEmpty(originLabels))  {
						categoryNames.addAll(originLabels);
					}
				}
				
				if (event.error_location != null) {
					Set<String> locationLabels = categories.getCategories(event.error_location.class_name);
					
					if (!CollectionUtil.safeIsEmpty(locationLabels))  {
						categoryNames.addAll(locationLabels);
					}
				}
			}
		}
		
		Collection<EventTypes> availTypes = eventInput.getEventTypes(); 
		
		if (availTypes.contains(EventTypes.EventTypes)) {
			
			appender.append(EventFilter.CRITICAL_EXCEPTIONS);

			for (String type : eventTypes) {
				appender.append(type);
			}
		}
		
		if (availTypes.contains(EventTypes.Tiers)) {
			
			appender.append(GroupSettings.toGroupName(EventFilter.APP_CODE));
			
			for (String categoryName : categoryNames) {
				appender.append(GroupSettings.toGroupName(categoryName));
			}
		}

		if (availTypes.contains(EventTypes.ExceptionTypes)) {
			for (String exceptionType : exceptionTypes) {
				appender.append(EventFilter.toExceptionFilter(exceptionType));
			}
		}
	}
}
