package com.takipi.integrations.grafana.functions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;

import com.sun.corba.se.spi.orbutil.fsm.Guard.Result;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.event.Location;
import com.takipi.api.client.data.event.Stats;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.ArrayUtils;
import com.takipi.integrations.grafana.utils.EventLinkEncoder;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class EventsFunction extends GrafanaFunction {

	private static final String STATS = Stats.class.getSimpleName().toLowerCase();

	private static final String LINK = "link";
	private static final String RATE = "rate";
	private static final String FIRST_SEEN = "first_seen";
	private static final String LAST_SEEN = "last_seen";
	private static final String MESSAGE = "message";

	protected class EventData {
		protected EventResult event;
		
		protected EventData(EventResult event) {
			this.event = event;
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
			return TimeUtils.prettifyTime((String) value);
		}
	}

	protected static class LinkFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			return EventLinkEncoder.encodeLink(serviceId, input, eventData.event, 
				timeSpan.getFirst(), timeSpan.getSecond());
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

		String[] columnsArray = ArrayUtils.safeSplitArray(columns, ARRAY_SEPERATOR, true);
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
		
		Collection<EventResult> events = getEventList(serviceId, input, TimeUtils.toTimespan(timeSpan), input.volumeType);
		
		if (events == null) {
			return Collections.emptyList();
		}
		
		List<EventData> result = new ArrayList<EventData>(events.size());
		
		for (EventResult event : events) {
			result.add(new EventData(event));
		}
		
		return result;
	}

	private List<List<Object>> processServiceEvents(String serviceId, EventsInput input, Pair<DateTime, DateTime> timeSpan,
			Collection<FieldFormatter> formatters) {

		Collection<EventData> eventDatas = getEventData(serviceId, input, timeSpan);
		
		EventFilter eventFilter = input.getEventFilter(serviceId);

		List<List<Object>> result = new ArrayList<List<Object>>(eventDatas.size());

		for (EventData eventData : eventDatas) {

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

	private static List<String> getColumns(String fields) {

		String[] fieldArray = ArrayUtils.safeSplitArray(fields, ARRAY_SEPERATOR, true);
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

		Pair<DateTime, DateTime> timeSpan = TimeUtils.getTimeFilter(input.timeFilter);
		Collection<FieldFormatter> formatters = getFieldFormatters(input.fields);

		Series series = new Series();

		series.name = SERIES_NAME;
		series.values = new ArrayList<List<Object>>();
		series.columns = getColumns(input.fields);

		String[] serviceIds = getServiceIds(input);

		for (String serviceId : serviceIds) {
			List<List<Object>> serviceEvents = processServiceEvents(serviceId, input, timeSpan, formatters);
			series.values.addAll(serviceEvents);
		}

		return Collections.singletonList(series);
	}
}
