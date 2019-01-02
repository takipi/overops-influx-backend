package com.takipi.integrations.grafana.functions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
import com.takipi.integrations.grafana.input.CostCalculatorInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.input.GeneralSettings;
import com.takipi.integrations.grafana.util.ArrayUtil;
import com.takipi.integrations.grafana.util.EventLinkEncoder;
import com.takipi.integrations.grafana.util.TimeUtil;

public class CostCalculatorFunction extends GrafanaFunction {

	private static final String STATS = Stats.class.getSimpleName().toLowerCase();

	protected static final String LINK = "link";
	protected static final String FIRST_SEEN = "first_seen";
	protected static final String LAST_SEEN = "last_seen";
	protected static final String MESSAGE = "message";
	protected static final String COST = "cost";
	protected static final String COSTPCT = "costpct";
	
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

		protected Object formatValue(Object value, CostCalculatorInput input) {

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

		protected abstract Object getValue(EventData eventData, String serviceId, CostCalculatorInput input,
				Pair<DateTime, DateTime> timeSpan);

		protected Object format(EventData eventData, String serviceId, CostCalculatorInput input, Pair<DateTime, DateTime> timeSpan) {

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
		protected Object getValue(EventData eventData, String serviceId, CostCalculatorInput input,
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
		protected Object formatValue(Object value, CostCalculatorInput input) {
			return TimeUtil.prettifyTime((String) value);
		}
	}

	protected class LinkFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, CostCalculatorInput input,
				Pair<DateTime, DateTime> timeSpan) {

			return EventLinkEncoder.encodeLink(apiClient, serviceId, input, eventData.event, 
					timeSpan.getFirst(), timeSpan.getSecond());
		}
		
		@Override
		protected Object formatValue(Object value, CostCalculatorInput input) {
			return value;
		}
	}
	
	protected static class MessageFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, CostCalculatorInput input,
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

	protected static class CostFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, CostCalculatorInput input,
				Pair<DateTime, DateTime> timeSpan) {

			if (eventData.event.stats.hits == 0) {
				return .0;
			}

			Double costPerHit = .0;
			String type = (eventData.event.type == null) ? "" : eventData.event.type.trim();
			
			if (input.costMatrix.containsKey(type)) {
				costPerHit = input.costMatrix.get(type);
			} else if (input.costMatrix.containsKey("")) {
				costPerHit = input.costMatrix.get("");
			} 

			if (costPerHit == null) {
				return .0;
			}

			double cost = (double) eventData.event.stats.hits * costPerHit;
			
			return cost;
		}

	}

	protected static class CostPctFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, CostCalculatorInput input,
				Pair<DateTime, DateTime> timeSpan) {

			return .0;
		}

	}

	protected FieldFormatter getFormatter(String column) {
		
		if (column.equals(LINK)) {
			return new LinkFormatter();
		}

		if (column.equals(COST)) {
			return new CostFormatter();
		}

		if (column.equals(COSTPCT)) {
			return new CostPctFormatter();
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
			return new CostCalculatorFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return CostCalculatorInput.class;
		}

		@Override
		public String getName() {
			return "costCalc";
		}
	}

	public CostCalculatorFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	protected Collection<EventData> getEventData(String serviceId, CostCalculatorInput input, 
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

	protected List<List<Object>> processServiceEvents(String serviceId, CostCalculatorInput input, Pair<DateTime, DateTime> timeSpan) {

		Collection<FieldFormatter> formatters = getFieldFormatters(input.fields);
		
		Collection<EventData> eventDatas = getEventData(serviceId, input, timeSpan);
		Collection<EventData> mergedDatas = mergeSimilarEvents(serviceId, eventDatas);
		
		EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);

		List<List<Object>> result = new ArrayList<List<Object>>(mergedDatas.size());
				
		int costFieldNr = getColumns(input.fields).indexOf("Cost");

		double runningCostTotal= .0;
		
		List<Object> outputObject;
		
		for (EventData eventData : mergedDatas) {
			
			if (eventFilter.filter(eventData.event)) {
				continue;
			}

			if (eventData.event.stats.hits == 0) {
				continue;
			}

			outputObject = processEvent(serviceId, input, eventData, formatters, timeSpan);

			if (costFieldNr > -1  && costFieldNr < outputObject.size()  && 
					outputObject.get(costFieldNr) instanceof Double &&
					outputObject.get(costFieldNr) != null) {
				runningCostTotal += (double) outputObject.get(costFieldNr);
			}

			if (!postCostRowFilter(outputObject, input))
				result.add(outputObject);
		}
		
		final double costTotal = runningCostTotal;
		
		final double targetTablelimit = runningCostTotal * input.tableLimit / 100.0;
		
		result.sort(new Comparator<List<Object>>() {

			@Override
			public int compare(List<Object> o1, List<Object> o2) {
				return (int)((Double)o2.get(4) - (Double)o1.get(4));
			}
		});

		List<List<Object>> sortedResult = new ArrayList<List<Object>>(mergedDatas.size());

		double runningcostTotal = .0;
		for (List <Object> res : result) {
			if (runningcostTotal <= targetTablelimit) {
				int costPctFieldNr = getColumns(input.fields).indexOf("Costpct");
				if (costPctFieldNr > -1 && costPctFieldNr < res.size() && res.get(costPctFieldNr) instanceof Double) {
					if (costTotal != .0) {
						res.set(costPctFieldNr, (Double) res.get(costFieldNr) / costTotal);
					} else {
						res.set(costPctFieldNr, "NA");
					}
				}
				runningcostTotal += (Double) res.get(costFieldNr);
				sortedResult.add(res);
			}
		};
		
		return sortedResult;

	}

	private boolean postCostRowFilter(List<Object> outputObject, CostCalculatorInput input) {
		List<String> col = getColumns(input.fields);
		int costFieldNr = col.indexOf("Cost");
		if (costFieldNr > -1  && costFieldNr < outputObject.size()  && outputObject.get(costFieldNr) instanceof Double)
			if ((double) outputObject.get(costFieldNr) <= input.costHigherThan) {
				return true;
			}
		
		return false;
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

	private List<Object> processEvent(String serviceId, CostCalculatorInput input, EventData eventData,
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

		if (!(functionInput instanceof CostCalculatorInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		CostCalculatorInput input = (CostCalculatorInput) functionInput;

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
