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
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.event.BaseStats;
import com.takipi.api.client.data.event.Location;
import com.takipi.api.client.data.event.MainEventStats;
import com.takipi.api.client.data.event.Stats;
import com.takipi.api.client.data.metrics.Graph;
import com.takipi.api.client.data.metrics.Graph.GraphPoint;
import com.takipi.api.client.data.metrics.Graph.GraphPointContributor;
import com.takipi.api.client.request.event.BreakdownType;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.result.event.EventSlimResult;
import com.takipi.api.client.result.event.EventsSlimVolumeResult;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.infra.Categories.CategoryType;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionUtil.RegressionWindow;
import com.takipi.api.client.util.settings.RegressionSettings;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.EventsInput.OutputMode;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.settings.ServiceSettings;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.EventLinkEncoder;
import com.takipi.integrations.grafana.util.TimeUtil;

public class EventsFunction extends GrafanaFunction {

	private static final String STATS = Stats.class.getSimpleName().toLowerCase();
	private static final String JIRA_LABEL = "JIRA";

	protected static final String FIRST_SEEN = "first_seen";
	protected static final String MESSAGE = "message";
	protected static final String JIRA_ISSUE_URL = "jira_issue_url";

	private static final String UNAMED_DEPLOMENT = "Unnamed Deployment";
	
	private static final int MAX_JIRA_BATCH_SIZE = 10;
	
	private static final int MAX_BASELINE_DAYS = 7;

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
		protected Stats baselineStats;
		protected Collection<EventData> mergedEvents;
		protected int rank;
		protected DateTime lastSeen;

		
		protected EventData(EventResult event) {		
			this.event = event;
			this.rank = -1;
		}
		
		@Override
		public boolean equals(Object obj) {
			
			if (obj == null || (!(obj instanceof EventData))) {
				return false;
			}
				
			EventData other = (EventData)obj;
			
			/** Removed for consistency with oo classic
			if (!Objects.equal(event.type, other.event.type)) {
				return false;
			}
			*/
			
			if (!compareEvents(event, other.event)) {
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
		public String toString() {
			if (event.entry_point != null) {
				return event.entry_point.class_name;
			}
			
			return super.toString();
		}
	}
	
	protected class EventsJiraAsyncTask extends BaseAsyncTask implements Callable<Object> {

		protected String serviceId;
		protected Collection<EventResult> events;
		protected EventsInput input;

		protected EventsJiraAsyncTask(String serviceId, EventsInput input, 
			Collection<EventResult> events) {

			this.serviceId = serviceId;
			this.input = input;
			this.events = events;
		}

		private String getJiraUrl(String serviceId, EventsInput input, String Id) {
			
			Response<EventResult> response = ApiCache.getEvent(apiClient, serviceId, Id, input.query);
			
			if ((response == null) || (response.data == null)) {
				return null;
			}
			
			if (response.data.jira_issue_url != null) {
				return response.data.jira_issue_url;
			}
			
			return null;
		}
		
		private String getEventJiraUrl(EventsInput input, EventResult event) {
			
			if (event.jira_issue_url != null) {
				return event.jira_issue_url;
			}
				
			if (!CollectionUtil.safeContains(event.labels, JIRA_LABEL)) {
				return null;
			}
			
			if (CollectionUtil.safeIsEmpty(event.similar_event_ids)) {
				return getJiraUrl(serviceId, input, event.id);
			}
			
			for (String similarId : event.similar_event_ids) {
				
				String jiraUrl = getJiraUrl(serviceId, input, similarId);
				
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
					String jiraUrl = getEventJiraUrl(input, event);
					
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
					builder.append(TEXT_SEPERATOR);
				}
			}

			return builder.toString();
		}
		
		protected static String getCleanClassName(EventsInput input, String value) {
			
			String result;
			
			if ((input.maxClassLength > 0)  && (value.length() > input.maxClassLength)) {		
				result = "..." + value.substring(
					value.length() - input.maxClassLength, value.length());
			} else {
				result = value;
			}
					
			return result;
		}
		
		protected Object formatValue(Object value, EventsInput input) {

			if (value == null) {
				return "";
			}

			if (value instanceof Location) {
				Location location = (Location)value;
				String simpleClassName = getSimpleClassName(location.class_name) + "." + location.method_name;
				String cleanClassName = getCleanClassName(input, simpleClassName);
				return cleanClassName;
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
		
		@Override
		protected Object formatValue(Object value, EventsInput input) {
			
			if ((value instanceof Long) && (((Long)value).longValue() == 0)) {
				return "NA";
			}
			
			if ((value instanceof Integer) && (((Integer)value).intValue() == 0)) {
				return "NA";
			}
			
			return value;
		}
	}

	protected static class FirstSeenFormatter extends ReflectFormatter {

		protected FirstSeenFormatter(Field field) {
			super(field);
		}

		@Override
		protected Object formatValue(Object value, EventsInput input) {
			
			if (value == null) {
				return null;
			}
						
			return TimeUtil.getEpoch(value.toString());
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
	
	protected class RankFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			return eventData.rank;
		}
	}
	
	protected class EntryPointNameFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			if (eventData.event.entry_point == null) {
				return "";
			}
			
			StringBuilder result = new StringBuilder();
			
			String simpleClassName = getSimpleClassName(eventData.event.entry_point.class_name);
			result.append(getCleanClassName(input, simpleClassName));
			
			if (!CollectionUtil.safeIsEmpty(eventData.mergedEvents)) {
				
				Set<String> entryPoints = new HashSet<String>();
				
				for (EventData merged : eventData.mergedEvents) {
					
					if (merged.event.entry_point == null) {
						continue;
					}
					
					entryPoints.add(formatLocation(merged.event.entry_point));
				}
				
				if (entryPoints.size() > 1) {
					result.append(" and ");
					result.append(entryPoints.size() - 1);
					result.append(" more");

				}	
			}
			
			return result.toString();
		}
	}
	
	protected static class EventTransactionsStats {
		protected List<String> ids;
		protected long volume;
		
		protected EventTransactionsStats() {
			this.ids = new ArrayList<String>();
		}
	}
	
	protected class EventDescriptionFormatter extends FieldFormatter {

		private Categories categories;
		
		protected EventDescriptionFormatter(Categories categories) {
			this.categories = categories;
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
			
			Set<String> labels = null;
			
			if (categories != null) {
				Set<String> originLabels;
				
				if (eventData.event.error_origin != null) {
					originLabels = categories.getCategories(
						eventData.event.error_origin.class_name, CategoryType.infra);
				} else {
					originLabels = null;
				}
						
				Set<String> locationLabels;
				
				if (eventData.event.error_location != null) {
					locationLabels = categories.getCategories(
						eventData.event.error_location.class_name, CategoryType.infra);
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
				
			if ((eventData.event.introduced_by != null) 
			&& (!UNAMED_DEPLOMENT.equals(eventData.event.introduced_by))) {
				result.append(". Introduced by: ");
				result.append(eventData.event.introduced_by);
			}
			
			if (!CollectionUtil.safeIsEmpty(labels)) {
				result.append(". Tier");
				
				if ((labels != null) && (labels.size() > 1)) {
					result.append("s");
				}
				result.append(": ");
				result.append(String.join(TEXT_SEPERATOR, labels));
			}
			
			if (!CollectionUtil.safeIsEmpty(eventData.event.labels)) {
				result.append(". Label");
				
				if (eventData.event.labels.size() > 1) {
					result.append("s");
				}
				result.append(": ");
				result.append(String.join(TEXT_SEPERATOR, eventData.event.labels));
			}
			
			boolean hasApps = false;
			
			if (!CollectionUtil.safeIsEmpty(eventData.event.stats.contributors)) {
				for (Stats stats : eventData.event.stats.contributors) {
					if ((stats.application_name != null) 
					|| (stats.deployment_name != null)
					|| (stats.machine_name != null)) {
						hasApps = true;
						break;
					}
				}		
			}
	
			if (hasApps) {		
				result.append(". Apps: ");
				
				int index = 0;
				
				for (Stats stats : eventData.event.stats.contributors) {
										
					if (stats.application_name != null) {
						
						if (eventData.event.stats.hits > 0) {
							double appPercentage = (double)stats.hits / (double)eventData.event.stats.hits * 100;
							
							result.append(stats.application_name);
							result.append(": ");
							
							if (appPercentage > 1) {
								result.append(singleDigitFormatter.format(appPercentage));
							} else {
								result.append(doubleDigitFormatter.format(appPercentage));	
							}
							
							result.append("%");
						}  else {
							result.append(stats.application_name);
						}
					}
					
					if (index < eventData.event.stats.contributors.size() - 1) {
						result.append(TEXT_SEPERATOR);
					}
					
					index++;	
				}
			}
							
			if (!CollectionUtil.safeIsEmpty(eventData.mergedEvents)) {
				
				long volume = 0;
				Map<String, EventTransactionsStats> transactionsStats = new TreeMap<String, EventTransactionsStats>();
				
				for (EventData mergedEvent : eventData.mergedEvents) {
					
					String key;
					
					if (mergedEvent.event.entry_point != null) {
						key = formatLocation(mergedEvent.event.entry_point);			
					} else {
						key = "Unknown transaction";	
					}
					
					EventTransactionsStats transactionStats = transactionsStats.get(key);
					
					if (transactionStats == null) {
						transactionStats = new EventTransactionsStats();
						transactionsStats.put(key, transactionStats);
					} 
					
					transactionStats.volume += mergedEvent.event.stats.hits;
					transactionStats.ids.add(mergedEvent.event.id);
					
					volume += mergedEvent.event.stats.hits;
				}
				
				result.append(". Transactions: ");
					
				int index = 0;
				
				for (Map.Entry<String, EventTransactionsStats> entry : transactionsStats.entrySet()) {
					
					result.append(entry.getKey());		
					result.append("(");
						
					if (volume > 0) {
						double entryPointPercentage = (double)entry.getValue().volume / (double)volume * 100;
						
						if (entryPointPercentage > 1) {
							result.append(singleDigitFormatter.format(entryPointPercentage));
						} else {
							result.append(doubleDigitFormatter.format(entryPointPercentage));	
						}
						
						result.append("%, ");
					}
					
					result.append(" Id");
					
					if (entry.getValue().ids.size() > 1) {
						result.append("s");	
					}
					
					result.append(": ");	
					result.append(String.join(TEXT_SEPERATOR, entry.getValue().ids));
					result.append(")");
										
					if (index < eventData.mergedEvents.size() - 1) {
						result.append(TEXT_SEPERATOR);
					}
					
					index++;
				}	
			} else {
				result.append(". Id: ");
				result.append(eventData.event.id);
			}
			
			return result.toString();
		}	
		
		@Override
		protected Object formatValue(Object value, EventsInput input) {
			return value;
		}
	}
	
	protected class JiraUrlFormatter extends ReflectFormatter {

		protected JiraUrlFormatter(Field field) {
			super(field);
		}

		@Override
		protected Object formatValue(Object value, EventsInput input) {
			if (value == null) {
				return super.formatValue(value, input);
			}
			
			String result = 	value.toString().replaceAll(HTTP, "").replaceAll(HTTPS, "");

			return result;
		}
	}
	
	protected class RateDeltaFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			Double deltaValue = getRateDelta(eventData);
			
			if (deltaValue == null) {
				return "";
			}
			
			return deltaValue;
		}
			
	}
	
	protected class RateDeltaDescFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			Pair<Double, Double> ratePair = getRatePair(eventData);
			
			if (ratePair == null) {
				return "";
			}
			
			return formatRateDelta(eventData.baselineStats, eventData.event.stats);
		}
		
		@Override
		protected Object formatValue(Object value, EventsInput input) {
			return value;
		}
	}
	
	protected class LastSeenFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			DateTime result;
			
			if (eventData.lastSeen != null) {
				result = eventData.lastSeen;
			} else {
				result = TimeUtil.getDateTime(eventData.event.first_seen);
			}
			
			return result;
			
		}
		
		@Override
		protected Object formatValue(Object value, EventsInput input) {
			
			if (value == null) {
				return null;
			}
						
			return ((DateTime)value).getMillis();

		}
	}
		
	protected class LinkFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			return EventLinkEncoder.encodeLink(apiClient, getSettingsData(serviceId), 
				serviceId, input, eventData.event, 
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

			if ((eventData.event.type.equals(HTTP_ERROR)) 
			|| (EXCEPTION_TYPES.contains(eventData.event.type))) {
				
				StringBuilder result = new StringBuilder();
				result.append(eventData.event.name);
				
				if (hasMessage) {
					result.append(": ");
					result.append(eventData.event.message);
				} else if (eventData.event.error_location != null) {
					result.append(" in ");
					result.append(formatLocation(eventData.event.error_location));
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
				Pair<DateTime, DateTime> timeSpan) {
			
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
				
				if (rate >= 1.0) {
					return "100%";
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

			if (eventData.event.stats.invocations == 0) {
				return formatLongValue(eventData.event.stats.hits);
			}
	
			return formatRate(eventData.event.stats);
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
		
		if (column.equals(EventsInput.LAST_SEEN)) {
			return new LastSeenFormatter();
		}
		
		if (column.equals(EventsInput.TYPE_MESSAGE)) {
			return new TypeMessageFormatter();
		}
		
		if (column.equals(EventsInput.DESCRIPTION)) {
			Categories categories = getSettings(serviceId).getCategories();
			return new EventDescriptionFormatter(categories);
		}
		
		if (column.equals(EventsInput.RATE_DELTA)) {
			return new RateDeltaFormatter();
		}
		
		if (column.equals(EventsInput.RATE_DELTA_DESC)) {
			return new RateDeltaDescFormatter();
		}
		
		if (column.equals(EventsInput.RANK)) {
			return new RankFormatter();
		}
		
		if (column.equals(EventsInput.ENTRY_POINT_NAME)) {
			return new EntryPointNameFormatter();
		}
			
		Field field = getReflectField(column);

		if (column.equals(JIRA_ISSUE_URL)) {
			return new JiraUrlFormatter(field);
		}
		
		if (column.equals(FIRST_SEEN))  {
			return new FirstSeenFormatter(field);
		}
		
		if (BaseStats.class.isAssignableFrom(field.getDeclaringClass())) {
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
	
	public EventsFunction(ApiClient apiClient, Map<String, ServiceSettings> settingsMaps) {
		super(apiClient, settingsMaps);
	}
	
	protected static String formatRateDelta(BaseStats baseline, BaseStats stats) {
		
		StringBuilder result = new StringBuilder();
		
		result.append(formatRate(stats));
		result.append(" up from ");
		result.append(formatRate(baseline));
	
		return result.toString(); 
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

	protected boolean compareEvents(EventResult e1, EventResult e2) {
		
		if (!equalLocations(e1.error_origin, e2.error_origin)) {
			return false;
		}
		
		if (!equalLocations(e1.error_location, e2.error_location)) {
			return false;
		}
		
		if (!Objects.equal(e1.call_stack_group, e2.call_stack_group)) {
			return false;
		}
		
		return true;
	}
	
	protected static String formatRate(BaseStats stats) {
		return formatRate(stats.hits, stats.invocations);
	}
	
	protected static String formatRate(long hits, long invocations) {
		
		if (invocations == 0) {
			return String.valueOf(hits);
		}
		
		double rate = (double)hits / (double)invocations;

		StringBuilder result = new StringBuilder();
		
		result.append(formatLongValue(hits));
		result.append(" in ");
		result.append(formatLongValue(invocations));
		result.append(" calls (");
		result.append(formatRate(rate, true));		
		result.append(")");
		
		return result.toString();
	}
	
	private static Pair<Double, Double> getRatePair(EventData eventData) {
		
		if (eventData.baselineStats == null) {
			return null;
		}
		
		if (eventData.baselineStats.invocations == 0) {
			return null;
		}
		
		if (eventData.event.stats.invocations == 0) {
			return null;
		}
		
		double baseRate = (double)eventData.baselineStats.hits / (double)eventData.baselineStats.invocations;
		double rate = (double)eventData.event.stats.hits / (double)eventData.event.stats.invocations;

		return Pair.of(baseRate, rate);
	}
	
	private static Double getRateDelta(EventData eventData) {
	
		Pair<Double, Double> ratePair = getRatePair(eventData);
		
		if (ratePair == null) {
			return null;
		}
		
		
		if (ratePair.getFirst() > ratePair.getSecond()) {
			return null;
			
		}
		double deltaValue = ratePair.getSecond() - ratePair.getFirst();
		
		if (deltaValue < 0.01) {
			return null;
		}
		
		return deltaValue;
	}
	
	
		
	private void updateLastSeenMap(String serviceId, EventsInput input, 
		Map<String, DateTime> map, long timeRange) {
		
		Gson gson = new Gson();
		String json = gson.toJson(input);
		EventsInput timeRangeInput = gson.fromJson(json, input.getClass());
		timeRangeInput.timeFilter = TimeUtil.getLastWindowTimeFilter(timeRange);
		
		Pair<DateTime, DateTime> timeRangePair = TimeUtil.getTimeFilter(timeRangeInput.timeFilter);
		
		Graph timeRangeGraph = getEventsGraph(serviceId, timeRangeInput, VolumeType.hits,
				timeRangePair.getFirst(), timeRangePair.getSecond());
		
		if (CollectionUtil.safeIsEmpty(timeRangeGraph.points)) {
			return;
		}
		
		for (GraphPoint gp : timeRangeGraph.points) {
			
			if (CollectionUtil.safeIsEmpty(gp.contributors)) {
				continue;
			}
			
			for (GraphPointContributor gpc : gp.contributors) {
				
				if (gpc.stats.hits == 0) {
					continue;
				}
				
				map.put(gpc.id, TimeUtil.getDateTime(gp.time));
			}
		}	
	}
	
	private boolean hasPointInRange(Collection<DateTime> values, long timeRange) {
		
		DateTime now = TimeUtil.now();
		
		for (DateTime lastSeen : values) {
			
			if (now.minus(timeRange).isBefore(lastSeen)) {
				return true;
			}
		}
		
		return false;
	}
	
	private void updateLastSeen(String serviceId, 
		EventsInput input, List<EventData> eventDatas) {
		
		Map<String, DateTime> lastSeenMap = new HashMap<String, DateTime>();
		
		updateLastSeenMap(serviceId, input, lastSeenMap, 
			TimeUnit.HOURS.toMillis(24 * 7 + 1));
		
		if (hasPointInRange(lastSeenMap.values(), TimeUnit.DAYS.toMillis(1))) {
			updateLastSeenMap(serviceId, input, lastSeenMap,
				TimeUnit.DAYS.toMillis(1));
		}
		
		if (hasPointInRange(lastSeenMap.values(), TimeUnit.HOURS.toMillis(1))) {
			updateLastSeenMap(serviceId, input, lastSeenMap, 
				TimeUnit.HOURS.toMillis(1));
		}
		
		for (EventData eventData : eventDatas) {
			eventData.lastSeen = lastSeenMap.get(eventData.event.id);
			
			if ((input.maxRows != 0) && (eventData.rank > input.maxRows)) {
				continue;
			}
			
			if (CollectionUtil.safeIsEmpty(eventData.mergedEvents)) {
				continue;
			}
			
			for (EventData mergedEvent : eventData.mergedEvents) {
				
				DateTime mergedLastSeen = lastSeenMap.get(mergedEvent.event.id);
				
				if (eventData.lastSeen != null) {
					if (mergedLastSeen != null) {
						eventData.lastSeen = new DateTime(
							Math.max(mergedLastSeen.getMillis(), 
									eventData.lastSeen.getMillis()));
					}
				} else {
					eventData.lastSeen = mergedLastSeen;
				}
			}
		}		
	}
	
	protected List<EventData> getEventData(String serviceId, EventsInput input, 
			Pair<DateTime, DateTime> timeSpan) {
		
		Set<BreakdownType> breakdownTypes;
		
		long delta = timeSpan.getSecond().getMillis() - timeSpan.getFirst().getMillis();
		
		if (delta <= TimeUnit.DAYS.toMillis(MAX_BASELINE_DAYS)) {
			breakdownTypes = Collections.singleton(BreakdownType.App);
		} else {
			breakdownTypes = Collections.emptySet();
		}
		
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, 
			timeSpan.getFirst(), timeSpan.getSecond(), input.volumeType, // VolumeType.hits,//
			false, breakdownTypes);
		
		if (eventsMap == null) {
			return Collections.emptyList();
		}
		
		List<EventData> result = new ArrayList<EventData>(eventsMap.size());
		
		for (EventResult event : eventsMap.values()) {
			result.add(new EventData(event));
		}
		
		return result;
	}
	
	/**
	 * @param serviceId - needed for children 
	 */
	@SuppressWarnings("unchecked")
	protected List<EventData> mergeSimilarEvents(String serviceId,
		boolean skipGrouping, List<EventData> eventDatas) {
				
		if (skipGrouping) {
			return eventDatas;
		}
		
		Map<EventData, Object> eventDataMap = new HashMap<EventData, Object>(eventDatas.size());
		
		for (EventData eventData : eventDatas) {
			
			Object eventDataMatch = eventDataMap.get(eventData);
			
			if (eventDataMatch instanceof List) {
				List<EventData> eventDataMatches = (List<EventData>)eventDataMatch;
				eventDataMatches.add(eventData);
			} else if (eventDataMatch instanceof EventData) {
				List<EventData> matchesList = new ArrayList<EventData>();
				matchesList.add((EventData)eventDataMatch);
				matchesList.add(eventData);
				eventDataMap.put(eventData, matchesList);		
			} else {
				eventDataMap.put(eventData, eventData);		
			}
		}
		
		List<EventData> result = new ArrayList<EventData>();
		
		for (Object similarEventDatas : eventDataMap.values()) {
			
			if (similarEventDatas instanceof List) {
				List<EventData> toMerge = (List<EventData>)similarEventDatas;
				result.addAll(mergeEventDatas(toMerge));
			} else {
				result.add((EventData)similarEventDatas);
			}
		}
		
		return result;
	}
	
	private void mergeSimilarIds(EventResult target, EventResult source) {
		
		if (source == null) {
			return;
		}
		
		Set<String> similarIds = new HashSet<String>();
		
		if (source.similar_event_ids != null) {
			similarIds.addAll(source.similar_event_ids);
		}
		
		if (target.similar_event_ids != null) {
			similarIds.addAll(target.similar_event_ids);
		}
		
		similarIds.add(source.id);
		
		target.similar_event_ids = new ArrayList<String>(similarIds);
	}
	
	private void appendStats(List <Stats> statsList, Stats stats) {
		
		Stats match = null;
		
		for (Stats item : statsList) {
			if ((Objects.equal(item.application_name, stats.application_name))
			&& (Objects.equal(item.deployment_name, stats.deployment_name))
			&& (Objects.equal(item.machine_name, stats.machine_name))) {
				match = item;
				break;
			}
		}
		
		if (match == null) {
			match = new Stats();
			match.application_name = stats.application_name;
			match.deployment_name = stats.deployment_name;
			match.machine_name = stats.machine_name;
			statsList.add(match);
		}
		
		match.hits += stats.hits;
		match.invocations += stats.invocations;	
	}
	
	protected List<EventData> mergeEventDatas(List<EventData> eventDatas) {
		
		if (eventDatas.size() == 0) {
			throw new IllegalArgumentException("eventDatas");
		}

		String jiraUrl = null;
		MainEventStats stats = new MainEventStats();
		stats.contributors = new ArrayList<Stats>();
		
		EventResult event = null;
		
		for (EventData eventData : eventDatas) {
		
			stats.hits += eventData.event.stats.hits;
			stats.invocations += eventData.event.stats.invocations;	

			if (!CollectionUtil.safeIsEmpty(eventData.event.stats.contributors)) {
				for (Stats sc : eventData.event.stats.contributors) {
					appendStats(stats.contributors, sc);
				}
			}
			
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
		
		clone = (EventResult)event.clone();
		
		clone.jira_issue_url = jiraUrl;
		
		EventData result = new EventData(clone);
		result.mergedEvents = eventDatas;
		
		return Collections.singletonList(result);
	}
	
	private void updateEventBaselineStats(String serviceId, 
		EventsInput input, Pair<DateTime, DateTime> timeSpan,
		Collection <EventData> eventDatas) {
		
		String viewId = getViewId(serviceId, input.view);
		
		if (viewId == null) {
			return;
		}
		
		RegressionFunction regressionFunction = new RegressionFunction(apiClient, settingsMaps);
		
		Pair<RegressionInput, RegressionWindow> regPair = regressionFunction.getRegressionInput(serviceId, 
			viewId, input, timeSpan, false);
		
		if (regPair == null) {
			return;
		}
		
		int baseline = regPair.getFirst().baselineTimespan;		
		Pair<DateTime, DateTime> baselineTimespan = Pair.of(timeSpan.getFirst().minusMinutes(baseline) ,timeSpan.getFirst());

		
		Gson gson = new Gson();
		String json = gson.toJson(input);
		EventsInput baselineInput = gson.fromJson(json, input.getClass());
		baselineInput.timeFilter = TimeUtil.toTimeFilter(baselineTimespan);
		baselineInput.deployments = null;
		
		EventsSlimVolumeResult eventsVolume = getEventsVolume(serviceId,
			viewId, baselineInput, baselineTimespan.getFirst(), baselineTimespan.getSecond(),
			VolumeType.all);

		Map<String, EventData> eventDataMap = new HashMap<String, EventData>(eventDatas.size());
		
		for (EventData eventData : eventDatas) {
			 
			eventDataMap.put(eventData.event.id, eventData);
			
			if (eventData.event.similar_event_ids != null) {
				for (String similarId : eventData.event.similar_event_ids) {
					eventDataMap.put(similarId, eventData);	
				}
			}
		}
		
		if (eventsVolume != null) {
			
			for (EventSlimResult eventResult : eventsVolume.events) {
				
				EventData eventData = eventDataMap.get(eventResult.id);
				
				if (eventData == null) {
					continue;
				}
					
				if (eventData.baselineStats == null) {
					eventData.baselineStats =  new Stats();
				}
				
				eventData.baselineStats.hits += eventResult.stats.hits;
				eventData.baselineStats.invocations += eventResult.stats.invocations;
			}
		}			
	}
	
	private void updateJiraUrls(String serviceId, 
		EventsInput input, Collection <EventData> eventDatas) {
		
		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
		
		List<EventResult> currentBatch = null;
		int index = 0;
		
		for (EventData eventData : eventDatas) {
			
			if ((input.maxRows != 0) && (eventData.rank > input.maxRows)) {
				continue;
			}
			
			if (eventData.event.jira_issue_url != null) {
				continue;
			}
			
			if ((CollectionUtil.safeIsEmpty(eventData.event.labels)) ||
				(!eventData.event.labels.contains(JIRA_LABEL))) {
				continue;
			}
			
			if (currentBatch == null) {
				currentBatch = new ArrayList<EventResult>();
				tasks.add(new EventsJiraAsyncTask(serviceId, input, currentBatch));
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
	
	private static double getDelta(Double value) {
		
		if (value == null) {
			return 0f;
		}
		
		return value.doubleValue();
		
	}
	
	
	protected void sortEventDatas(String serviceId, List<EventData> eventDatas ) {
	
		RegressionSettings regressionSettings = getSettingsData(serviceId).regression;
		
		int minThreshold = regressionSettings.error_min_volume_threshold;
		List<String> criticalExceptionList = new ArrayList<String>(regressionSettings.getCriticalExceptionTypes());
		
		eventDatas.sort(new Comparator<EventData>() {
			
			@Override
			public int compare(EventData o1, EventData o2) {
			
				double o1RateDelta = getDelta(getRateDelta(o1));
				double o2RateDelta = getDelta(getRateDelta(o2));
				
				return compareEvents(o1.event, o2.event, o1RateDelta, o2RateDelta, 
					criticalExceptionList, minThreshold);
			}
				
		});
		
		int index = 1;
		
		for (EventData eventData : eventDatas) {
			eventData.rank = index;
			index++;
		}
	}

	/**
	 * @param serviceIds - needed for children
	 */
	protected List<List<Object>> processServiceEvents(Collection<String> serviceIds, 
		String serviceId, EventsInput input, Pair<DateTime, DateTime> timeSpan) {
		
		List<EventData> eventDatas = processEventDatas(serviceId, 
			input, timeSpan);
				
		List<List<Object>> result = new ArrayList<List<Object>>(eventDatas.size());
			
		Map<String, FieldFormatter> formatters = getFieldFormatters(serviceId, input.getFields());

		if ((formatters.containsKey(EventsInput.RATE_DELTA)) 
		|| (formatters.containsKey(EventsInput.RANK)) 
		|| (formatters.containsKey(EventsInput.RATE_DELTA_DESC))) {
		
			long delta = timeSpan.getSecond().getMillis() - timeSpan.getFirst().getMillis();
			
			if (delta <= TimeUnit.DAYS.toMillis(MAX_BASELINE_DAYS)) {
				updateEventBaselineStats(serviceId, input, timeSpan, eventDatas);
			}
		}
		
		sortEventDatas(serviceId, eventDatas);
		
		if ((formatters.containsKey(JIRA_ISSUE_URL)) 
		|| (formatters.containsKey(EventsInput.JIRA_STATE))) {
			updateJiraUrls(serviceId, input, eventDatas);
		}
				
		if (formatters.containsKey(EventsInput.LAST_SEEN)) { 
			updateLastSeen(serviceId, input, eventDatas);
		}
		
		int index = 0;
		
		for (EventData eventData : eventDatas) {	 
	
			List<Object> outputObject = processEvent(serviceId, 
				input, eventData, formatters.values(), timeSpan);
			
			if (outputObject != null) {
				result.add(outputObject);
			}
			
			index++;
			
			if ((input.maxRows != 0) && (index > input.maxRows)) {
				break;
			}
		}

		return result;

	}
	
	private List<EventData> processEventDatas(String serviceId, 
		EventsInput input, Pair<DateTime, DateTime> timeSpan) {
		
		List<EventData> mergedDatas;
		List<EventData> eventDatas = getEventData(serviceId, input, timeSpan);

		if (input.hasTransactions()) {
			mergedDatas = eventDatas;
		} else {
			mergedDatas = mergeSimilarEvents(serviceId, input.skipGrouping, eventDatas);
		}
					
		EventFilter eventFilter = getEventFilter(serviceId, input, timeSpan);

		if (eventFilter == null) {
			return Collections.emptyList();
		}
		
		List<EventData> result = new ArrayList<EventData>(mergedDatas.size());
			
		for (EventData eventData : mergedDatas) {	 
	
			if (eventFilter.filter(eventData.event)) {
				continue;
			}

			if (eventData.event.stats.hits == 0) {
				//continue;
			}
					
			result.add(eventData);
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
			clazz = BaseStats.class;
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
	
	private List<Series> processGrid(EventsInput input, Pair<DateTime, DateTime> timeSpan) {
		
		Series series = createSeries(new ArrayList<List<Object>>(), getColumns(input));
		
		Collection<String> serviceIds = getServiceIds(input);

		List<List<List<Object>>> servicesValues = new ArrayList<List<List<Object>>>(serviceIds.size());
		
		for (String serviceId : serviceIds) {
			List<List<Object>> serviceEvents = processServiceEvents(serviceIds, serviceId, input, timeSpan);
			series.values.addAll(serviceEvents);
			servicesValues.add(serviceEvents);
		}

		sortSeriesValues(series.values, servicesValues);
		
		return Collections.singletonList(series);
	}
	
	public int getEventCount(EventsInput input, Pair<DateTime, DateTime> timeSpan) {
	
		Collection<String> serviceIds = getServiceIds(input);

		int result = 0;
		
		for (String serviceId : serviceIds) {
			Collection<EventData> serviceEventDatas = processEventDatas(serviceId, input, timeSpan);
			result += serviceEventDatas.size();
		}
		
		return result;
		
	}
	
	private List<Series> processSingleStat(EventsInput input, Pair<DateTime, DateTime> timeSpan) {
	
		int value = getEventCount(input, timeSpan);
		return createSingleStatSeries(timeSpan, value);
	}

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof EventsInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		EventsInput input = (EventsInput)getInput((ViewInput)functionInput);
		
		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
  
		OutputMode outputMode = input.getOutputMode();
		
		List<Series> result;
			
		switch (outputMode) {
				
			case Grid:
				result = processGrid(input, timeSpan);
				break;
			case SingleStat:
				result = processSingleStat(input, timeSpan);
				break;
			default:
				throw new IllegalStateException(String.valueOf(outputMode));
		}
		
		return result;
	}
}
