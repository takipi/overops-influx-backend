package com.takipi.integrations.grafana.functions;

import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.event.Location;
import com.takipi.common.api.data.event.Stats;
import com.takipi.common.api.result.event.EventResult;
import com.takipi.common.api.util.Pair;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.QueryResult;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.utils.ArrayUtils;
import com.takipi.integrations.grafana.utils.EventLinkEncoder;
import com.takipi.integrations.grafana.utils.TimeUtils;

public class EventsFunction extends GrafanaFunction {

	private static final String link = "link";
	private static final Field LINK_FIELD;

	private static final String rate = "rate";
	private static final Field RATE_FIELD;

	private static final String STATS = Stats.class.getSimpleName().toLowerCase();

	private static final DecimalFormat df = new DecimalFormat("##.##%");

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

	private List<List<Object>> processServiceEvents(String serviceId, EventsInput request,
			Pair<String, String> timeSpan, Field[] fields) {

		List<EventResult> events = getEventList(serviceId, request, timeSpan);

		if (events == null) {
			return Collections.emptyList();
		}

		List<List<Object>> result = new ArrayList<List<Object>>(events.size());

		for (EventResult event : events) {
			List<Object> outputObject = processEvent(serviceId, request, event, fields, timeSpan);
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

	private static String formatFieldValue(Object value, Field field) {

		if (value instanceof Location) {
			Location location = (Location) value;
			int sepIdex = Math.max(location.class_name.lastIndexOf('.') + 1, 0);
			return location.class_name.substring(sepIdex, location.class_name.length());
		}

		if (value instanceof List) {
			return formatList(value);
		}

		if ((field.getName().equals("first_seen")) 
		|| (field.getName().equals("last_seen"))) {

			if (value != null) {
				return TimeUtils.prettifyTime((String) value);
			}
		}

		if (value == null) {
			return "";
		}

		return value.toString();
	}

	private static String parseStats(Stats stats) {
		if (stats.invocations > 0) {
			double value = (double) stats.hits / (double) stats.invocations;

			if (value < 0.01) {
				return "< 0.01%";
			} else {
				return df.format(value);
			}
		} else {
			return "";
		}
	}
	
	private Object formatObject(String serviceId, EventsInput request, EventResult event, Field field, Pair<String, String> timeSpan) {
		
		if (field.equals(LINK_FIELD)) {
			return EventLinkEncoder.encodeLink(serviceId, request, event, timeSpan);
		} 
		
		if (field.equals(RATE_FIELD)) {
			return parseStats(event.stats);
		}
			
		Object reflectValue = getReflectValue(field, event);
		return formatFieldValue(reflectValue, field);
	}

	private List<Object> processEvent(String serviceId, EventsInput request, EventResult event, Field[] fields,
			Pair<String, String> timeSpan) {

		List<Object> result = new ArrayList<Object>(fields.length);

		for (Field field : fields) {

			Object objectValue = formatObject(serviceId, request, event, field, timeSpan);
			result.add(objectValue);
		}

		return result;
	}

	private static Object getReflectValue(Field field, EventResult event) {
		Object target;

		if (field.getDeclaringClass().equals(Stats.class)) {
			target = event.stats;
		} else {
			target = event;
		}

		Object result;

		try {
			result = field.get(target);
		} catch (Exception e) {
			throw new IllegalStateException(e);
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

	private static Field[] getReflectFields(String columns) {

		if ((columns == null) || (columns.isEmpty())) {
			throw new IllegalArgumentException("columns cannot be empty");
		}

		String[] columnsArray = ArrayUtils.safeSplitArray(columns, ARRAY_SEPERATOR, true);
		Field[] result = new Field[columnsArray.length];

		for (int i = 0; i < columnsArray.length; i++) {

			Field field;
			String column = columnsArray[i];

			if (column.equals(link)) {
				field = LINK_FIELD;
			} else if (column.equals(rate)) {
				field = RATE_FIELD;
			} else {
				field = getReflectField(column);
			}

			result[i] = field;
		}

		return result;
	}

	@Override
	public QueryResult process(FunctionInput functionInput) {
		if (!(functionInput instanceof EventsInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		EventsInput request = (EventsInput) functionInput;

		Pair<String, String> timeSpan = TimeUtils.parseTimeFilter(request.timeFilter);
		Field[] fields = getReflectFields(request.fields);

		Series series = new Series();

		series.name = SERIES_NAME;
		series.values = new ArrayList<List<Object>>();
		series.columns = getColumns(request.fields);

		String[] services = getServiceIds(request);

		for (String serviceId : services) {
			List<List<Object>> serviceObjects = processServiceEvents(serviceId, request, timeSpan, fields);
			series.values.addAll(serviceObjects);
		}

		return createQueryResults(Collections.singletonList(series));
	}

	static {
		try {
			LINK_FIELD = EventsFunction.class.getDeclaredField(link);
			RATE_FIELD = EventsFunction.class.getDeclaredField(rate);
		} catch (Exception e) {
			throw new IllegalStateException();
		}
	}
}
