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

import org.joda.time.DateTime;
import org.ocpsoft.prettytime.PrettyTime;

import com.google.common.base.Objects;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.event.Location;
import com.takipi.api.client.data.event.Stats;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.CostCalculatorInput;
import com.takipi.integrations.grafana.input.CostSettings;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.input.GeneralSettings;
import com.takipi.integrations.grafana.util.ArrayUtil;
import com.takipi.integrations.grafana.util.EventLinkEncoder;
import com.takipi.integrations.grafana.util.TimeUtil;

public class CostCalculatorFunction extends GrafanaFunction {

	private static final String STATS = Stats.class.getSimpleName().toLowerCase();

	protected static final String FIRST_SEEN = "first_seen";
	protected static final String LAST_SEEN = "last_seen";
	protected static final String MESSAGE = "message";
	protected static final String COST = "cost";
	protected static final String COSTPCT = "costpct";
	protected static final String COSTYRL = "costyrl";

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
			if (obj == null ||
				(!(obj instanceof EventData))) {
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
					return str.substring(0, input.maxColumnLength) + "...";

				} else {
					return str;
				}

			}

			return value;
		}

		protected abstract Object getValue(EventData eventData, ApiClient apiClient, String serviceId, CostCalculatorInput input,
				Pair<DateTime, DateTime> timeSpan);

		protected Object format(EventData eventData, ApiClient apiClient, String serviceId, CostCalculatorInput input, Pair<DateTime, DateTime> timeSpan) {

			Object fieldValue = getValue(eventData, apiClient, serviceId, input, timeSpan);
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
		protected Object getValue(EventData eventData, ApiClient apiClient, String serviceId, CostCalculatorInput input,
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

	protected class EventDescriptionFormatter extends FieldFormatter {

		private Categories categories;
		private PrettyTime prettyTime;
		
		protected EventDescriptionFormatter(Categories categories) {
			this.categories = categories;
			this.prettyTime = new PrettyTime();
		}
		
		@Override
		protected Object getValue(EventData eventData, ApiClient apiClient, String serviceId, CostCalculatorInput input,
				Pair<DateTime, DateTime> timeSpan) {

			
			StringBuilder result = new StringBuilder();
			
			result.append(eventData.event.type);
			
			if (eventData.event.error_location !=  null) {
				result.append(" from ");
				result.append(getSimpleClassName(eventData.event.error_location.class_name));
				result.append(".");
				result.append(eventData.event.error_location.method_name);
			}
			
			if (eventData.event.entry_point !=  null) {
				result.append(" in transaction ");
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
				result.append(". Introduced by: ");
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
		protected Object formatValue(Object value, CostCalculatorInput input)
		{
			return value;
		}
	}
	
	protected class LinkFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, ApiClient apiClient, String serviceId, CostCalculatorInput input,
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
		protected Object getValue(EventData eventData, ApiClient apiClient, String serviceId, CostCalculatorInput input,
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
	
	protected static class TypeMessageFormatter extends MessageFormatter {
		
		@Override
		protected Object getValue(EventData eventData, ApiClient apiClient, String serviceId, CostCalculatorInput input,
				Pair<DateTime, DateTime> timeSpan)
		{
			String type = getTypesMap().get(eventData.event.type);
			Object value = super.getValue(eventData, apiClient, serviceId, input, timeSpan);

			String result;
		
			if (type != null) {
				result = type + ": " + value;
			} else {
				result = value.toString();
			}
			
			String location = formatLocation(eventData.event.error_location);
			
			if (result.length() + location.length() < input.maxColumnLength) {
				result += " in " + location;
			}
			
			return result;
		}
	}

	protected static class CostFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, ApiClient apiClient, String serviceId, CostCalculatorInput input,
				Pair<DateTime, DateTime> timeSpan) {

			Double result = .0;
			if (eventData.event.stats.hits == 0) {
				return .0;
			}

			CostSettings costData = GrafanaSettings.getData(apiClient, serviceId).cost_calculator;
			if (costData != null) {
				result = costData.calculateCost(eventData.event.type)
						* eventData.event.stats.hits;
			}
			return result;
		}

	}

	protected static class CostPctFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, ApiClient apiClient, String serviceId, CostCalculatorInput input,
				Pair<DateTime, DateTime> timeSpan) {

			return .0;
		}

	}

	protected static class CostYrlFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, ApiClient apiClient, String serviceId, CostCalculatorInput input,
				Pair<DateTime, DateTime> timeSpan) {

			return .0;
		}

	}

	protected FieldFormatter getFormatter(ApiClient apiClient, String serviceId, String column) {
		
		if (column.equals(CostCalculatorInput.LINK)) {
			return new LinkFormatter();
		}

		if (column.equals(COST)) {
			return new CostFormatter();
		}

		if (column.equals(COSTPCT)) {
			return new CostPctFormatter();
		}

		if (column.equals(COSTYRL)) {
			return new CostYrlFormatter();
		}
		
		if (column.equals(MESSAGE)) {
			return new MessageFormatter();
		}
		
		if (column.equals(CostCalculatorInput.TYPE_MESSAGE)) {
			return new TypeMessageFormatter();
		}
		
		if (column.equals(CostCalculatorInput.DESCRIPTION)) {
			Categories categories = GrafanaSettings.getServiceSettings(apiClient, serviceId).getCategories();
			return new EventDescriptionFormatter(categories);
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

	private Map<String, FieldFormatter> getFieldFormatters(ApiClient apiClient, String serviceId, String columns) {

		if ((columns == null) || (columns.isEmpty())) {
			throw new IllegalArgumentException("columns cannot be empty");
		}

		String[] columnsArray = ArrayUtil.safeSplitArray(columns, ARRAY_SEPERATOR, true);
		Map<String, FieldFormatter> result = new LinkedHashMap<String, FieldFormatter>(columnsArray.length);

		for (String column : columnsArray) {
			FieldFormatter fieldFormatter = getFormatter(apiClient, serviceId, column);
			result.put(column, fieldFormatter);
		}

		return result;
	}

	public CostCalculatorFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	protected List<EventData> getEventData(ApiClient apiClient, String serviceId, CostCalculatorInput input, 
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
	protected List<List<Object>> processServiceEvents(Collection<String> serviceIds, ApiClient apiClient, String serviceId, CostCalculatorInput input, Pair<DateTime, DateTime> timeSpan) {

		Map<String, FieldFormatter> formatters = getFieldFormatters(apiClient, serviceId, input.fields);
		
		List<EventData> mergedDatas;
		
		CostSettings costSettings = GrafanaSettings.getData(apiClient, serviceId).cost_calculator;

		List<EventData> eventDatas = getEventData(apiClient, serviceId, input, timeSpan);

		if (input.hasTransactions()) {
			mergedDatas = eventDatas;
		} else {
			mergedDatas = mergeSimilarEvents(serviceId, eventDatas);
		}
		
		sortEventDatas(mergedDatas);
			
		EventFilter eventFilter = input.getEventFilter(apiClient, serviceId);

		List<List<Object>> result = new ArrayList<List<Object>>(mergedDatas.size());
				
		int costFieldNr = getColumns(input.fields).indexOf("Cost");

		double runningCostTotal= .0;
		
		List<Object> outputObject;
		
		if (costFieldNr > -1) {
			for (EventData eventData : mergedDatas) {
				
				if (eventFilter.filter(eventData.event)) {
					continue;
				}
	
				if (eventData.event.stats.hits == 0) {
					continue;
				}
	
				outputObject = processEvent(serviceId, input, eventData, formatters.values(), timeSpan);
	
				if (outputObject != null) {
					if (costFieldNr < outputObject.size()) {
						Object costField = outputObject.get(costFieldNr);
						
						if (costField != null &&
								costField instanceof Double) {
							Double costDbl = (Double) costField;
							
							runningCostTotal += costDbl;
			
							if (costDbl >= costSettings.costHigherThan) {
								result.add(outputObject);
							}
						}
					}
				}
			}
		}
		
		final double costTotal = runningCostTotal;
		
		final double targetTablelimit = runningCostTotal * Math.min(100.0,Math.max(.01,input.tableLimit)) / 100.0;
		
		result.sort(new Comparator<List<Object>>() {

			@Override
			public int compare(List<Object> o1, List<Object> o2) {
				return (int)((Double)o2.get(costFieldNr) - (Double)o1.get(costFieldNr));
			}
		});

		List<List<Object>> sortedResult = new ArrayList<List<Object>>(mergedDatas.size());

		double runningcostTotal = .0;
		int costPctFieldNr = getColumns(input.fields).indexOf("Costpct");
		int costYrlFieldNr = getColumns(input.fields).indexOf("Costyrl");
		long intervalLen = timeSpan.getSecond().getMillis() - timeSpan.getFirst().getMillis() + 1L;
		long millisInYr = timeSpan.getSecond().getMillis() - timeSpan.getSecond().minusYears(1).getMillis();
		double intervalFactor = 1.0 * millisInYr / intervalLen;
		for (List <Object> res : result) {
			if (runningcostTotal <= targetTablelimit) {
				if (costPctFieldNr > -1 && costPctFieldNr < res.size() && res.get(costPctFieldNr) instanceof Double) {
					if (costTotal != .0) {
						res.set(costPctFieldNr, (Double) res.get(costFieldNr) / costTotal);
					} else {
						res.set(costPctFieldNr, "NA");
					}
				}
				
				if (costYrlFieldNr > -1 && costYrlFieldNr < res.size() && res.get(costYrlFieldNr) instanceof Double) {
					res.set(costYrlFieldNr, (Double) res.get(costFieldNr) * intervalFactor);
				}
				runningcostTotal += (Double) res.get(costFieldNr);
				sortedResult.add(res);
			} else {
				break;
			}
		}
		
		return sortedResult;

	}

//	private boolean postCostRowFilter(List<Object> outputObject, CostCalculatorInput input) {
//		List<String> col = getColumns(input.fields);
//		int costFieldNr = col.indexOf("Cost");
//		if (costFieldNr > -1  && costFieldNr < outputObject.size()  && outputObject.get(costFieldNr) instanceof Double)
//			if ((double) outputObject.get(costFieldNr) <= input.costData.costHigherThan) {
//				return true;
//			}
//		
//		return false;
//	}

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

			Object objectValue = formatter.format(eventData, apiClient, serviceId, input, timeSpan);
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
			List<List<Object>> serviceEvents = processServiceEvents(serviceIds, apiClient, serviceId, input, timeSpan);
			series.values.addAll(serviceEvents);
		}

		return Collections.singletonList(series);
	}
}
