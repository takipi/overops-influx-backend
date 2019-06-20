package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.joda.time.DateTime;

import com.google.gson.internal.LinkedTreeMap;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.settings.SlowdownSettings;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsFeedInput;
import com.takipi.integrations.grafana.input.EventsFeedInput.EventType;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.RegressionsInput;
import com.takipi.integrations.grafana.input.ReliabilityReportInput.ReliabilityKpi;
import com.takipi.integrations.grafana.input.TransactionsListInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.TimeUtil;

public class EventsFeedFunction extends RegressionFunction {	

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new EventsFeedFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EventsFeedInput.class;
		}

		@Override
		public String getName() {
			return "eventsFeed";
		}
	}
	
	protected abstract class BaseEventFeedTask extends BaseAsyncTask {
		
		protected String serviceId;
		protected Pair<DateTime, DateTime> timeSpan;
		protected EventsFeedInput input;
		
		protected BaseEventFeedTask(String serviceId, 
			Pair<DateTime, DateTime> timeSpan, EventsFeedInput input) {
			this.input = input;
			this.serviceId = serviceId;
			this.timeSpan = timeSpan;
		}
	}
	
	protected class RegressionTask extends BaseEventFeedTask  {

		protected Pair<List<EventData>, Map<String, FieldFormatter>> output;
		
		@Override
		public Object call() throws Exception {
			this.output = processServiceEventDatas(serviceId, input, timeSpan);
			return null;
		}
		
		protected RegressionTask(String serviceId, 
			Pair<DateTime, DateTime> timeSpan, EventsFeedInput input) {
			super(serviceId, timeSpan, input);
		}
		
	}
	
	protected class TransactionTask extends BaseEventFeedTask  {

		protected Pair<List<TransactionData>, RegressionInput> output;
		
		@Override
		public Object call() throws Exception {
			
			String json = gson.toJson(input);
			
			TransactionsListInput tlInput = gson.fromJson(json, TransactionsListInput.class);			
			TransactionsListFunction function = new TransactionsListFunction(apiClient);
			
			this.output = function.processServiceTransactionsDatas(serviceId, timeSpan, tlInput, 
				Collections.emptyList(), TransactionsListInput.getStates(input.performanceStates));
			
			return this;
		}
		
		protected TransactionTask(String serviceId, 
			Pair<DateTime, DateTime> timeSpan, EventsFeedInput input) {
			super(serviceId, timeSpan, input);
		}
	}
	
	protected class SlowdownEventData extends EventData {

		protected TransactionData transactionData;
		
		protected SlowdownEventData(TransactionData transactionData) {
			super(null);
			this.transactionData = transactionData;
		}	
	}
	
	protected class EventFeedMessageFormatter extends MessageFormatter {
		
		RegressionFullRateFormatter regressionFullRateFormatter;
		
		protected EventFeedMessageFormatter() {
			regressionFullRateFormatter = new RegressionFullRateFormatter();
		} 
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			StringBuilder result = new StringBuilder();
			
			if (eventData instanceof RegressionData) {
				
				Object eventDesc = super.getValue(eventData, serviceId, input, timeSpan);
				RegressionData regData = (RegressionData)eventData;
				
				if (regData.regression == null) {
					result.append(eventDesc);
				} else {
					result.append(eventDesc);
					result.append( " up to ");
					result.append(regressionFullRateFormatter.getValue(eventData, 
						serviceId, input, timeSpan));

				}
			} else if (eventData instanceof SlowdownEventData) {
				SlowdownEventData slowdownEventData = (SlowdownEventData)eventData;
				TransactionData transactionData = slowdownEventData.transactionData;
				
				String name = getTransactionName(transactionData.graph.name, true);
				
				double avgTime = transactionData.stats.avg_time;				
				double baselineTime = transactionData.baselineStats.avg_time;
				
				result.append(name);
				result.append(" ");
				result.append(formatMilli(baselineTime));
				result.append(" -> ");
				result.append(formatMilli(avgTime));
			}
			
			return result.toString();
		}
	}

	protected class EventFeedSeverityFormatter extends RegressionSeverityFormatter {
		
		protected SlowdownSettings slowdownSettings;

		protected EventFeedSeverityFormatter(SlowdownSettings slowdownSettings) {
			super();
			this.slowdownSettings = slowdownSettings;
		}
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			if (eventData instanceof RegressionData) {
				return super.getValue(eventData, serviceId, input, timeSpan);
			}
			
			if (eventData instanceof SlowdownEventData) {
				SlowdownEventData slowdownEventData = (SlowdownEventData)eventData;
				TransactionData transactionData = slowdownEventData.transactionData;
				
				switch (transactionData.state) {
					case CRITICAL:
						return P1;
					case SLOWING:
						return P2;
					default:
						return 0;
					
				}
			}
							
			return 0;
		}
	}

	protected class EventFeedTypeFormatter extends FieldFormatter {
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			if (eventData instanceof RegressionData) {
				RegressionData regData = (RegressionData)eventData;
				
				if (regData.regression == null) {
					return EventType.NewError.ordinal();
				} else {
					return EventType.IncError.ordinal();

				}
			} else if (eventData instanceof SlowdownEventData) {
				return EventType.Slowdown.ordinal();
			}
			
			return 0;
		}
	}
	
	protected class EventFeedLinkFormatter extends LinkFormatter {
		
		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			if (eventData instanceof RegressionData) {
				RegressionData regData = (RegressionData)eventData;
				
				if (regData.regression == null) {
					return super.getValue(eventData, serviceId, input, timeSpan);
				} else {
					
				}
			}
			
			if (eventData instanceof SlowdownEventData) {
				
			}
			
			return null;
		}
	}
	
	protected class EventFeedDescFormatter extends EventDescriptionFormatter {
		
		protected SlowdownSettings slowdownSettings;
		
		protected EventFeedDescFormatter(Categories categories, SlowdownSettings slowdownSettings) {
			super(categories);
			this.slowdownSettings = slowdownSettings;
		}

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {
			
			if (eventData instanceof RegressionData) {
				return super.getValue(eventData, serviceId, input, timeSpan);
			}
			
			if (eventData instanceof SlowdownEventData) {
				SlowdownEventData slowdownEventData = (SlowdownEventData)eventData;
				return slowdownEventData.transactionData.getSlowdownDesc(slowdownSettings);
			}
			
			return null;
		}
	}
	
	public EventsFeedFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	private void addEventDatas(EventsInput input, Pair<DateTime, DateTime> timeSpan,
		Map<String, Pair<RegressionTask, TransactionTask>> tasksMap,
		List<List<Object>> values, Collection<ReliabilityKpi> kpis, boolean newEvents) {
		
		for (Map.Entry<String, Pair<RegressionTask, TransactionTask>> entry : tasksMap.entrySet()) {
			
			String serviceId = entry.getKey();
			
			Pair<List<EventData>, Map<String, FieldFormatter>> output = entry.getValue().getFirst().output;
			
			List<EventData> evnetDatas = output.getFirst();
			Map<String, FieldFormatter> formatters = output.getSecond();
			
			for (EventData eventData : evnetDatas) {
				
				if (!(eventData instanceof RegressionData)) {
					continue;
				}
				
				RegressionData regressionData = (RegressionData)eventData;
				boolean isRegression = regressionData.regression != null;
				
				if ((newEvents) && (isRegression)) {
					continue;
				} 
				
				if ((!newEvents) && (!isRegression)) {
					continue;
				}
					
				switch (regressionData.type) {
					case NewIssues:
						if (!kpis.contains(ReliabilityKpi.NewErrors)) {
							continue;
						}
						break;
					case Regressions:
						if (!kpis.contains(ReliabilityKpi.ErrorIncreases)) {
							continue;
						}
						break;
					case SevereNewIssues:
						if (!kpis.contains(ReliabilityKpi.SevereNewErrors)) {
							continue;
						}
						break;
					case SevereRegressions:
						if (!kpis.contains(ReliabilityKpi.SevereErrorIncreases)) {
							continue;
						}
						break;
					default:
						continue;	
				}
				
				List<Object> object = processEvent(serviceId, input, eventData,
					formatters.values(), timeSpan);				
				
				if (object != null) {
					values.add(object);
				}
			}
		}
	}
	
	private void addSlowdowns(EventsInput input, Pair<DateTime, DateTime> timeSpan,
		Map<String, Pair<RegressionTask, TransactionTask>> tasksMap,
		List<List<Object>> values, Collection<ReliabilityKpi> kpis) {
			
		for (Map.Entry<String, Pair<RegressionTask, TransactionTask>> entry : tasksMap.entrySet()) {
			String serviceId = entry.getKey();
			
			Pair<List<TransactionData>, RegressionInput> output = entry.getValue().getSecond().output;
			List<TransactionData> transactionDatas = output.getFirst();
			
			Map<String, FieldFormatter> formatters = getFieldFormatters(serviceId, input.getFields());

			for (TransactionData transactionData : transactionDatas) {
				
				switch (transactionData.state) {
					case CRITICAL:
						if (!kpis.contains(ReliabilityKpi.SevereSlowdowns)) {
							continue;
						}
						break;
					case SLOWING:
						if (!kpis.contains(ReliabilityKpi.Slowdowns)) {
							continue;
						}
					default:
						continue;		
				}

				List<Object> newObject = processEvent(serviceId, 
					input, new SlowdownEventData(transactionData), formatters.values(), timeSpan);
						
				values.add(newObject);
			}
		}
	}
	
	@Override
	protected FieldFormatter getFormatter(String serviceId, String column) {
		
		if (column.equals(RegressionsInput.SEVERITY)) {
			SlowdownSettings slowdownSettings = getSettingsData(serviceId).slowdown;
			return new EventFeedSeverityFormatter(slowdownSettings);
		}
		
		if (column.equals(EventsFeedInput.EVENT_TYPE)) {
			return new EventFeedTypeFormatter();
		}
		
		if (column.equals(EventsFeedInput.MESSAGE)) {
			return new EventFeedMessageFormatter();
		}

		if (column.equals(EventsInput.LINK)) {
			return new EventFeedLinkFormatter();
		}
	
		if (column.equals(EventsInput.DESCRIPTION)) {
			Categories categories = getSettings(serviceId).getCategories();
			SlowdownSettings slowdownSettings = getSettingsData(serviceId).slowdown;
			return new EventFeedDescFormatter(categories, slowdownSettings);
		}
		
		return super.getFormatter(serviceId, column);
	}
	
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof EventsFeedInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		EventsFeedInput input = (EventsFeedInput)functionInput;
		
		Pair<DateTime, DateTime> timeSpan = TimeUtil.getTimeFilter(input.timeFilter);
  
		Collection<String> serviceIds = getServiceIds(input);
		Collection<ReliabilityKpi> kpis = input.getKpis();
		
		Map<String, Pair<RegressionTask, TransactionTask>> tasksMap = 
			new LinkedTreeMap<String, Pair<RegressionTask, TransactionTask>>();
		
		List<Callable<Object>> tasks = new ArrayList<Callable<Object>>();
		
		for (String serviceId : serviceIds) {
			
			RegressionTask regressionTask = new RegressionTask(serviceId, timeSpan, input);
			TransactionTask transactionTask = new TransactionTask(serviceId, timeSpan, input);

			tasksMap.put(serviceId, Pair.of(regressionTask, transactionTask));
			
			tasks.add(regressionTask);
			tasks.add(transactionTask);
		}
		
		executeTasks(tasks, true);
				
		List<List<Object>> values = new ArrayList<List<Object>>();
		
		addEventDatas(input, timeSpan, tasksMap, values, kpis, true);
		addEventDatas(input, timeSpan, tasksMap, values, kpis, false);
		addSlowdowns(input, timeSpan, tasksMap, values, kpis);
		
		return Collections.singletonList(createSeries(values, getColumns(input)));
	}	
}

