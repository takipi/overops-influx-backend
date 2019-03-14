package com.takipi.integrations.grafana.functions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;
import org.ocpsoft.prettytime.PrettyTime;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.event.Location;
import com.takipi.api.client.data.event.Stats;
import com.takipi.api.client.request.event.EventRequest;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.regression.RegressionStringUtil;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.input.GeneralSettings;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.EventLinkEncoder;
import com.takipi.integrations.grafana.util.TimeUtil;

public class EventsFunction extends GrafanaFunction {

	private static final String STATS = Stats.class.getSimpleName().toLowerCase();
	private static final String JIRA_LABEL = "JIRA";

	protected static final String FIRST_SEEN = "first_seen";
	protected static final String LAST_SEEN = "last_seen";
	protected static final String MESSAGE = "message";
	protected static final String JIRA_ISSUE_URL = "jira_issue_url";
	
	private static final int MAX_JIRA_BATCH_SIZE = 10;

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new EventsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EventsInput.class;
		}

		@Override
		public String getName() {
			return "events";
		}
	}

	
	protected class EventData {
		protected EventResult event;
		
		protected EventData(EventResult event) {
			this.event = event;
		}
		
		private boolean equalLocations(Location a, Location b) {
			
			if (a == null) {
				return b == null;
			} 
			
			if (b == null) {
				return false;
			} 
			
			if (!Objects.equal(a.class_name, b.class_name)) {
				return false;
			}

			if (!Objects.equal(a.method_name, b.method_name)) {
				return false;
			}

			if (!Objects.equal(a.method_desc, b.method_desc)) {
				return false;
			}

			return true;
		}
		
		@Override
		public boolean equals(Object obj) {
			
			if (obj == null || (!(obj instanceof EventData))) {
				return false;
			}
				
			EventData other = (EventData)obj;
			
			if (!Objects.equal(event.type, other.event.type)) {
				return false;
			}
			
			if (!equalLocations(event.error_origin, other.event.error_origin)) {
				return false;
			}
			
			if (!equalLocations(event.error_location, other.event.error_location)) {
				return false;
			}
			
			if (!Objects.equal(event.call_stack_group, other.event.call_stack_group)) {
				return false;
			}
			
			return true;	
		}
		
		@Override
		public int hashCode() {
			
			if (event.error_location == null) {
				return super.hashCode();
			}
			
			return event.error_location.class_name.hashCode();
		}
		
		@Override
		public String toString()
		{
			if (event.entry_point != null) {
				return event.entry_point.class_name;
			}
			
			return super.toString();
		}
	}
	
	protected class EventsJiraAsyncTask extends BaseAsyncTask implements Callable<Object> {

		protected String serviceId;
		protected Collection<EventResult> events;

		protected EventsJiraAsyncTask(String serviceId, Collection<EventResult> events) {

			this.serviceId = serviceId;
			this.events = events;
		}

		private String getJiraUrl(String serviceId, String id) {
			
			Response<EventResult> response = apiClient.get(EventRequest.newBuilder().setServiceId(serviceId).setEventId(id).build());
			
			if ((response == null) || (response.data == null)) {
				return null;
			}
			
			if (response.data.jira_issue_url != null) {
				return response.data.jira_issue_url;
			}
			
			return null;
		}
		
		private String getEventJiraUrl(EventResult event) {
			
			if (event.jira_issue_url != null) {
				return event.jira_issue_url;
			}
				
			if (!CollectionUtil.safeContains(event.labels, JIRA_LABEL)) {
				return null;
			}
			
			if (CollectionUtil.safeIsEmpty(event.similar_event_ids)) {
				return getJiraUrl(serviceId, event.id);
			}
			
			for (String similarId : event.similar_event_ids) {
				
				String jiraUrl = getJiraUrl(serviceId, similarId);
				
				if (jiraUrl != null ) {
					return jiraUrl;
				}
			}
			
			return null;
		}
		
		
		@Override
		public Object call() {

			beforeCall();

			try {
				
				Map<String, String> result = new HashMap<String, String>();
				
				for (EventResult event : events) {
					String jiraUrl = getEventJiraUrl(event);
					
					if (jiraUrl != null) {
						result.put(event.id, jiraUrl) ;
					}
				}

				return result;
			} finally {
				afterCall();
			}

		}

		@Override
		public String toString() {
			return String.join(" ", "Jira Urls", serviceId);
		}
	}
	
	protected abstract static class FieldFormatter {

		private static String formatList(Object value) {

			@SuppressWarnings("unchecked")
			List<Object> list = (List<Object>) value;

			StringBuilder builder = new StringBuilder();

			for (int i = 0; i < list.size(); i++) {
				builder.append(list.get(i));

				if (i < list.size() - 1) {
					builder.append(", ");
				}
			}

			return builder.toString();
		}

		protected Object formatValue(Object value, EventsInput input) {

			if (value == null) {
				return "";
			}

			if (value instanceof Location) {
				Location location = (Location)value;
				return getSimpleClassName(location.class_name) + "." + location.method_name;
			}

			if (value instanceof List) {
				return formatList(value);
			}

			if ((value instanceof String) && (input.maxColumnLength > 0)) {

				String str = (String) value;

				if (str.length() > input.maxColumnLength) {
					return str.substring(0, input.maxColumnLength) + "...";

				} else {
					return str;
				}

			}

			return value;
		}

		protected abstract Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan);

		protected Object format(EventData eventData, String serviceId, EventsInput input, Pair<DateTime, DateTime> timeSpan) {

			Object fieldValue = getValue(eventData, serviceId, input, timeSpan);
			return formatValue(fieldValue, input);
		}
	}

	protected static class ReflectFormatter extends FieldFormatter {

		private Field field;

		protected ReflectFormatter(Field field) {
			this.field = field;
		}
		
		protected Object getTarget(EventData eventData) {
			return eventData.event;
		}

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			try {
				Object target = getTarget(eventData);
				return field.get(target);
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
	}
	
	protected static class StatsFormatter extends ReflectFormatter {

		protected StatsFormatter(Field field) {
			super(field);
		}
		
		@Override
		protected Object getTarget(EventData eventData) {
			return eventData.event.stats;
		}	
	}

	protected static class DateFormatter extends ReflectFormatter {

		protected DateFormatter(Field field) {
			super(field);
		}

		@Override
		protected Object formatValue(Object value, EventsInput input) {
			return TimeUtil.prettifyTime((String) value);
		}
	}

	protected class JiraStateFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			if (eventData.event.jira_issue_url !=  null) {
				return 1;
			}
			
			return "";
		}
	}
	
	protected class EventDescriptionFormatter extends FieldFormatter {

		private Categories categories;
		private PrettyTime prettyTime;
		
		protected EventDescriptionFormatter(Categories categories) {
			this.categories = categories;
			this.prettyTime = new PrettyTime();
		}
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			
			StringBuilder result = new StringBuilder();
			
			result.append(eventData.event.type);
			
			if (eventData.event.error_location !=  null) {
				result.append(" in ");
				result.append(getSimpleClassName(eventData.event.error_location.class_name));
				result.append(".");
				result.append(eventData.event.error_location.method_name);
			}
			
			if (eventData.event.entry_point !=  null) {
				result.append(" from ");
				result.append(getSimpleClassName(eventData.event.entry_point.class_name));
			}
			
			Set<String> labels = null;
			
			if (categories != null) {
				Set<String> originLabels;
				
				if (eventData.event.error_origin != null) {
					originLabels = categories.getCategories(eventData.event.error_origin.class_name);
				} else {
					originLabels = null;
				}
						
				Set<String> locationLabels;
				
				if (eventData.event.error_location != null) {
					locationLabels = categories.getCategories(eventData.event.error_location.class_name);
				} else {
					locationLabels = null;	
				}
					
				if (locationLabels != null) {
						
					if (originLabels != null) {
						labels = new HashSet<>(locationLabels.size() + originLabels.size());
						labels.addAll(locationLabels);
						labels.addAll(originLabels);
						
					} else {
						labels = locationLabels;
					}
				} 
				else {
					labels = originLabels;
				}
			}
				
			if (eventData.event.introduced_by != null) {
				result.append(". New in: ");
				result.append(eventData.event.introduced_by);
			} else {
				result.append(". First seen: ");
				DateTime firstSeen = TimeUtil.getDateTime(eventData.event.first_seen);
				result.append(prettyTime.format(firstSeen.toDate()));	
			}
			
			if (!CollectionUtil.safeIsEmpty(labels)) {
				result.append(". Tier");
				
				if ((labels != null) && (labels.size() > 1)) {
					result.append("s");
				}
				result.append(": ");
				result.append(String.join(", ", labels));
			}
			
			return result.toString();
		}
		
		@Override
		protected Object formatValue(Object value, EventsInput input)
		{
			return value;
		}
	}
	
	protected class JiraUrlFormatter extends ReflectFormatter {

		protected JiraUrlFormatter(Field field)
		{
			super(field);
		}

		@Override
		protected Object formatValue(Object value, EventsInput input)
		{
			if (value == null) {
				return super.formatValue(value, input);
			}
			
			String result = 	value.toString().replaceAll(HTTP, "").replaceAll(HTTPS, "");

			return result;
		}
	}
	
	
	protected class LinkFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			return EventLinkEncoder.encodeLink(apiClient, serviceId, input, eventData.event, 
				timeSpan.getFirst(), timeSpan.getSecond());
		}
		
		@Override
		protected Object formatValue(Object value, EventsInput input) {
			return value;
		}
	}
	
	protected static class MessageFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			boolean hasMessage = (eventData.event.message !=  null) 
				&& (!eventData.event.message.trim().isEmpty());

			if (eventData.event.type.toLowerCase().contains("exception")) {
				
				StringBuilder result = new StringBuilder();
				result.append(eventData.event.name);
				
				if (hasMessage) {
					result.append(": ");
					result.append(eventData.event.message);
				} else {
					if (eventData.event.error_location != null) {
						result.append(" in ");
						result.append(formatLocation(eventData.event.error_location));
					}
				}
				
				return result.toString();
			} else {
				
				if (hasMessage) {
					return eventData.event.message;
				} else {
					return eventData.event.type;
				}		
			}			
		}

	}
	
	protected static class TypeMessageFormatter extends MessageFormatter {
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan)
		{
			String type = TYPES_MAP.get(eventData.event.type);
			Object value = super.getValue(eventData, serviceId, input, timeSpan);

			String result;
		
			if (type != null) {
				result = type + ": " + value;
			} else {
				result = value.toString();
			}
			
			String location = formatLocation(eventData.event.error_location);
			
			if ((location != null) && (result.length() + location.length() < input.maxColumnLength)) {
				result += " in " + location;
			}
			
			return result;
		}
	}

	protected static class RateFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			if (eventData.event.stats.invocations > 0) {
				double rate = (double) eventData.event.stats.hits / (double) eventData.event.stats.invocations;
				
				if (rate > 10) {
					return "> 1000%";
				}
								
				return rate;
			} else {
				return "NA";
			}
		}

	}
	
	protected static class RateDescFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			return RegressionStringUtil.getEventRate(eventData.event, true);
		}

	}

	protected FieldFormatter getFormatter(String serviceId, String column) {
		
		if (column.equals(EventsInput.LINK)) {
			return new LinkFormatter();
		}

		if (column.equals(EventsInput.RATE)) {
			return new RateFormatter();
		}
		
		if (column.equals(EventsInput.RATE_DESC)) {
			return new RateDescFormatter();
		}	
		
		if (column.equals(EventsInput.JIRA_STATE)) {
			return new JiraStateFormatter();
		}
		
		if (column.equals(MESSAGE)) {
			return new MessageFormatter();
		}
		
		if (column.equals(EventsInput.TYPE_MESSAGE)) {
			return new TypeMessageFormatter();
		}
		
		if (column.equals(EventsInput.DESCRIPTION)) {
			Categories categories = GrafanaSettings.getServiceSettings(apiClient, serviceId).getCategories();
			return new EventDescriptionFormatter(categories);
		}
			
		Field field = getReflectField(column);

		if (column.equals(JIRA_ISSUE_URL)) {
			return new JiraUrlFormatter(field);
		}
		
		if ((column.equals(FIRST_SEEN)) || (column.equals(LAST_SEEN))) {
			return new DateFormatter(field);
		}
		
		if (field.getDeclaringClass().equals(Stats.class)) {
			return new StatsFormatter(field);
		}

		return new ReflectFormatter(field);

	}

	private Map<String, FieldFormatter> getFieldFormatters(String serviceId, Collection<String> columns) {
		
		Map<String, FieldFormatter> result = new LinkedHashMap<String, FieldFormatter>(columns.size());

		for (String column : columns) {
			FieldFormatter fieldFormatter = getFormatter(serviceId, column);
			result.put(column, fieldFormatter);
		}

		return result;
	}
	
	public EventsFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	protected List<EventData> getEventData(String serviceId, EventsInput input, 
			Pair<DateTime, DateTime> timeSpan) {
		
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timeSpan.getFirst(), 
			timeSpan.getSecond(), input.volumeType, input.pointsWanted);
		
		if (eventsMap == null) {
			return Collections.emptyList();
		}
		
		List<EventData> result = new ArrayList<EventData>(eventsMap.size());
		
		for (EventResult event : eventsMap.values()) {
			result.add(new EventData(event));
		}
		
		return result;
	}
	
	protected List<EventData> mergeSimilarEvents(String serviceId, List<EventData> eventDatas) {
		
		GeneralSettings settings = GrafanaSettings.getData(apiClient, serviceId).general;
		
		if ((settings == null) || (!settings.group_by_entryPoint)) {
			return eventDatas;
		}
		
		Map<EventData, List<EventData>> eventDataMap = new HashMap<EventData, List<EventData>>(eventDatas.size());
		
		for (EventData eventData : eventDatas) {
			
			List<EventData> eventDataMatches = eventDataMap.get(eventData);
			
			if (eventDataMatches == null) {
				eventDataMatches = new ArrayList<EventData>();
				eventDataMap.put(eventData, eventDataMatches);		
			}
			
			eventDataMatches.add(eventData);
		}
		
		List<EventData> result = new ArrayList<EventData>();
		
		for (List<EventData> similarEventDatas : eventDataMap.values()) {
			
			if (similarEventDatas.size() > 1) {
				result.addAll(mergeEventDatas(similarEventDatas));
			} else {
				result.add(similarEventDatas.get(0));
			}
		}
		
		return result;
	}
	
	private void mergeSimilarIds(EventResult target, EventResult source) {
		
		if (source == null) {
			return;
		}
		
		if (source.similar_event_ids != null) {
			if (target.similar_event_ids != null) {
				for (String similarId: source.similar_event_ids) {
					if (!target.similar_event_ids.contains(similarId)) {
						target.similar_event_ids.add(similarId);
					}
				}
			} else {
				target.similar_event_ids = new ArrayList<String>(source.similar_event_ids);	
			}
		} else if (target.similar_event_ids == null) {
			target.similar_event_ids = new ArrayList<String>();
		}
		
		if (!target.similar_event_ids.contains(source.id)) {
			target.similar_event_ids.add(source.id);
		}
	}
	
	protected List<EventData> mergeEventDatas(List<EventData> eventDatas) {
		
		if (eventDatas.size() == 0) {
			throw new IllegalArgumentException("eventDatas");
		}

		String jiraUrl = null;
		Stats stats = new Stats();
		
		EventResult event = null;
		
		for (EventData eventData : eventDatas) {
		
			stats.hits += eventData.event.stats.hits;
			stats.invocations += eventData.event.stats.invocations;	

			if ((event == null) || (eventData.event.stats.hits > event.stats.hits)) {
				mergeSimilarIds(eventData.event, event);
				event = 	eventData.event;	
			} else {
				mergeSimilarIds(event, eventData.event);
			}
			
			if (event.jira_issue_url != null) {
				jiraUrl = event.jira_issue_url;
			}
		}
		
		if (event == null) {
			throw new IllegalStateException();
		}
		
		EventResult clone;
		
		try {
			clone = (EventResult)event.clone();
		} catch (CloneNotSupportedException e) {
			throw new IllegalStateException(e);
		}
		
		clone.stats = stats;
		clone.jira_issue_url = jiraUrl;
		return Collections.singletonList(new EventData(clone));
	}

	private void updateJiraUrls(String serviceId, Collection <EventData> eventDatas) {
		
		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
		
		List<EventResult> currentBatch = null;
		int index = 0;
		
		for (EventData eventData : eventDatas) {
			
			if (eventData.event.jira_issue_url != null) {
				continue;
			}
			
			if ((CollectionUtil.safeIsEmpty(eventData.event.labels)) ||
				(!eventData.event.labels.contains(JIRA_LABEL))) {
				continue;
			}
			
			if (currentBatch == null) {
				currentBatch = new ArrayList<EventResult>();
				tasks.add(new EventsJiraAsyncTask(serviceId, currentBatch));
			}
			
			index++;
			currentBatch.add(eventData.event);
			
			if (index == MAX_JIRA_BATCH_SIZE) {
				index = 0;
				currentBatch = null;
			}
		}
		
		List<Object> taskResults = executeTasks(tasks, true);

		Map<String, String> eventUrlMap = new HashMap<String, String>();
		
		for (Object taskResult : taskResults) {
			 
			if (!(taskResult instanceof Map<?, ?>)) {
				continue;
			}
			 
			@SuppressWarnings("unchecked")
			Map<String, String> taskUrlMap = (Map<String, String>)taskResult;
			eventUrlMap.putAll(taskUrlMap);
		 }
			 
		for (EventData eventData : eventDatas) {
			String eventUrl = eventUrlMap.get(eventData.event.id);
			
			if (eventUrl != null) {
				eventData.event.jira_issue_url = eventUrl;
			}
		}
	}
	
	protected void sortEventDatas(List<EventData> eventDatas ) {
	
		eventDatas.sort(new Comparator<EventData>()
		{
			@Override
			public int compare(EventData o1, EventData o2)
			{
				return (int)(o2.event.stats.hits - o1.event.stats.hits);
			}
		});
	}
	
	/**
	 * @param serviceIds - needed for children
	 */
	protected List<List<Object>> processServiceEvents(Collection<String> serviceIds, String serviceId, EventsInput input, Pair<DateTime, DateTime> timeSpan) {

		Map<String, FieldFormatter> formatters = getFieldFormatters(serviceId, input.getFields());
		
		List<EventData> mergedDatas;
		List<EventData> eventDatas = getEventData(serviceId, input, timeSpan);

		if (input.hasTransactions()) {
			mergedDatas = eventDatas;
		} else {
			mergedDatas = mergeSimilarEvents(serviceId, eventDatas);
		}
		
		sortEventDatas(mergedDatas);
			
		EventFilter eventFilter = getEventFilter(serviceId, input, timeSpan);

		if (eventFilter == null) {
			return Collections.emptyList();
		}
		
		List<List<Object>> result = new ArrayList<List<Object>>(mergedDatas.size());
			
		if ((formatters.containsKey(JIRA_ISSUE_URL)) 
		|| (formatters.containsKey(EventsInput.JIRA_STATE))) {
			updateJiraUrls(serviceId, eventDatas);
		}
		
		for (EventData eventData : mergedDatas) {	 
	
			if (eventFilter.filter(eventData.event)) {
				continue;
			}

			if (eventData.event.stats.hits == 0) {
				continue;
			}
			
			List<Object> outputObject = processEvent(serviceId, input, eventData, formatters.values(), timeSpan);
			
			if (outputObject != null) {
				result.add(outputObject);
			}
		}

		return result;

	}

	/**
	 * @param input - needed for children  
	 */
	protected List<String> getColumns(EventsInput input) {

		List<String> fields = input.getFields();
		List<String> result = new ArrayList<String>(fields.size());

		for (String field : fields) {

			String fieldValue;

			int fieldIndex = field.indexOf('.');

			if (fieldIndex == -1) {
				fieldValue = field;
			} else {
				fieldValue = field.substring(fieldIndex + 1, field.length());
			}

			String prettyField = fieldValue.substring(0, 1).toUpperCase()
					+ fieldValue.substring(1, fieldValue.length());

			result.add(prettyField.replace('_', ' '));
		}

		return result;
	}

	private List<Object> processEvent(String serviceId, EventsInput input, EventData eventData,
			Collection<FieldFormatter> formatters, Pair<DateTime, DateTime> timeSpan) {

		List<Object> result = new ArrayList<Object>(formatters.size());

		for (FieldFormatter formatter : formatters) {

			Object objectValue = formatter.format(eventData, serviceId, input, timeSpan);
			result.add(objectValue);
		}

		return result;
	}

	private static Field getReflectField(String column) {

		Class<?> clazz;
		String fieldName;

		if (column.startsWith(STATS)) {
			clazz = Stats.class;
			fieldName = column.substring(column.indexOf(".") + 1);
		} else {
			clazz = EventResult.class;
			fieldName = column;
		}

		Field field;

		try {
			field = clazz.getField(fieldName);
		} catch (Exception e) {
			throw new IllegalStateException("Field " + fieldName + " not found", e);
		}

		return field;
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof EventsInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		EventsInput input = (EventsInput) functionInput;

		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);

		Series series = new Series();

		series.name = SERIES_NAME;
		series.values = new ArrayList<List<Object>>();
		series.columns = getColumns(input);

		Collection<String> serviceIds = getServiceIds(input);

		for (String serviceId : serviceIds) {
			List<List<Object>> serviceEvents = processServiceEvents(serviceIds, serviceId, input, timeSpan);
			series.values.addAll(serviceEvents);
		}

		return Collections.singletonList(series);
	}
}
