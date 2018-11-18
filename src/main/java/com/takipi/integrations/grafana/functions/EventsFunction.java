package com.takipi.integrations.grafana.functions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.event.Location;
import com.takipi.api.client.data.event.Stats;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.EventSettings;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.util.ArrayUtil;
import com.takipi.integrations.grafana.util.EventLinkEncoder;
import com.takipi.integrations.grafana.util.TimeUtil;

public class EventsFunction extends GrafanaFunction {

	private static final String STATS = Stats.class.getSimpleName().toLowerCase();

	protected static final String LINK = "link";
	protected static final String RATE = "rate";
	protected static final String FIRST_SEEN = "first_seen";
	protected static final String LAST_SEEN = "last_seen";
	protected static final String MESSAGE = "message";

	protected class EventData {
		protected EventResult event;
		
		protected EventData(EventResult event) {
			this.event = event;
		}
		
		@Override
		public boolean equals(Object obj) {
			
			EventData other = (EventData)obj;
			
			if (!Objects.equal(event.type, other.event.type)) {
				return false;
			}
			
			if (!Objects.equal(event.error_origin, other.event.error_origin)) {
				return false;
			}
			
			if (!Objects.equal(event.error_location, other.event.error_location)) {
				return false;
			}
			
			return true;	
		}
		
		@Override
		public int hashCode() {
			
			if (event.error_location == null) {
				return super.hashCode();
			}
			
			return event.error_location.hashCode();
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
				return formatLocation((Location) value);
			}

			if (value instanceof List) {
				return formatList(value);
			}

			if ((value instanceof String) && (input.maxColumnLength > 0)) {

				String str = (String) value;

				if (str.length() > input.maxColumnLength) {
					return str.substring(0, input.maxColumnLength);

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

	protected static class LinkFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			return EventLinkEncoder.encodeLink(serviceId, input, eventData.event, 
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

	protected static class RateFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			if (eventData.event.stats.invocations > 0) {
				double rate = (double) eventData.event.stats.hits / (double) eventData.event.stats.invocations;
				return rate;
			} else {
				return "NA";
			}
		}

	}

	protected FieldFormatter getFormatter(String column) {
		
		if (column.equals(LINK)) {
			return new LinkFormatter();
		}

		if (column.equals(RATE)) {
			return new RateFormatter();
		}
		
		if (column.equals(MESSAGE)) {
			return new MessageFormatter();
		}
		
		Field field = getReflectField(column);

		if ((column.equals(FIRST_SEEN)) || (column.equals(LAST_SEEN))) {
			return new DateFormatter(field);
		}
		
		if (field.getDeclaringClass().equals(Stats.class)) {
			return new StatsFormatter(field);
		}

		return new ReflectFormatter(field);

	}

	private Collection<FieldFormatter> getFieldFormatters(String columns) {

		if ((columns == null) || (columns.isEmpty())) {
			throw new IllegalArgumentException("columns cannot be empty");
		}

		String[] columnsArray = ArrayUtil.safeSplitArray(columns, ARRAY_SEPERATOR, true);
		List<FieldFormatter> result = new ArrayList<FieldFormatter>(columnsArray.length);

		for (int i = 0; i < columnsArray.length; i++) {
			String column = columnsArray[i];
			FieldFormatter fieldFormatter = getFormatter(column);
			result.add(fieldFormatter);
		}

		return result;
	}

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

	public EventsFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	protected Collection<EventData> getEventData(String serviceId, EventsInput input, 
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
	
	protected Collection<EventData> mergeSimilarEvents(String serviceId, Collection<EventData> eventDatas) {
		
		EventSettings settings = GrafanaSettings.getServiceSettings(apiClient, serviceId).gridSettings;
		
		if ((settings == null) || (!settings.collapseEventsByEntryPoint)) {
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
				result.add(mergeEventDatas(similarEventDatas));
			} else {
				result.add(similarEventDatas.get(0));
			}
		}
		
		return result;
	}
	
	protected EventData mergeEventDatas(List<EventData> eventDatas) {
		
		Stats stats = new Stats();
		EventResult event = null;
		
		for (EventData eventData : eventDatas) {
			
			if (eventData.event.error_location.class_name.contains("DiskBac")) {
				System.out.println();
			}
			
			stats.hits += eventData.event.stats.hits;
			stats.invocations += eventData.event.stats.invocations;	
			
			if ((event == null) || (eventData.event.stats.hits > event.stats.hits)) {
				event = 	eventData.event;
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
		return new EventData(event);
	}

	protected List<List<Object>> processServiceEvents(String serviceId, EventsInput input, Pair<DateTime, DateTime> timeSpan) {

		Collection<FieldFormatter> formatters = getFieldFormatters(input.fields);
		
		Collection<EventData> eventDatas = getEventData(serviceId, input, timeSpan);
		Collection<EventData> mergedDatas = mergeSimilarEvents(serviceId, eventDatas);
		
		EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);

		List<List<Object>> result = new ArrayList<List<Object>>(mergedDatas.size());
		
		for (EventData eventData : mergedDatas) {
			
			if (eventFilter.filter(eventData.event)) {
				continue;
			}

			if (eventData.event.stats.hits == 0) {
				continue;
			}

			List<Object> outputObject = processEvent(serviceId, input, eventData, formatters, timeSpan);
			result.add(outputObject);
		}

		return result;

	}

	protected List<String> getColumns(String fields) {

		String[] fieldArray = ArrayUtil.safeSplitArray(fields, ARRAY_SEPERATOR, true);
		List<String> result = new ArrayList<String>(fieldArray.length);

		for (String field : fieldArray) {

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
		series.columns = getColumns(input.fields);

		Collection<String> serviceIds = getServiceIds(input);

		for (String serviceId : serviceIds) {
			List<List<Object>> serviceEvents = processServiceEvents(serviceId, input, timeSpan);
			series.values.addAll(serviceEvents);
		}

		return Collections.singletonList(series);
	}
}
