package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.EventFilterInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.SlowTransactionsInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.TimeUtil;

public class SlowTransactionsFunction extends EnvironmentVariableFunction
{

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new SlowTransactionsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return SlowTransactionsInput.class;
		}

		@Override
		public String getName() {
			return "slowTransactions";
		}
	}
	
	public SlowTransactionsFunction(ApiClient apiClient)
	{
		super(apiClient);
	}
	
	public Collection<String> getSlowTransactions(String serviceId, 
			EventFilterInput input, Pair<DateTime, DateTime> timeSpan, int limit)
	{		
		Map<String, EventResult> eventsMap = getEventMap(serviceId, input, timeSpan.getFirst(), timeSpan.getSecond(), 
			VolumeType.hits, input.pointsWanted);
		
		if (eventsMap == null) {
			return Collections.emptyList();
		}
	
		List<EventResult> timers = new ArrayList<EventResult>(eventsMap.size());
		
		for (EventResult event : eventsMap.values()) {
			
			if (!TIMER.equals(event.type)) {
				continue;
			}
			
			if (event.entry_point == null) {
				continue;
			}
			
			timers.add(event);
		}
		
		timers.sort(new Comparator<EventResult>()
		{

			@Override
			public int compare(EventResult o1, EventResult o2)
			{
				return (int)(o2.stats.hits - o1.stats.hits);
			}
		});
				
		List<String> result = new ArrayList<String>();
		
		for (int i = 0; i < timers.size(); i++) {
			EventResult timer = timers.get(i);
			String transaction = getSimpleClassName(timer.entry_point.class_name);
			
			if (!result.contains(transaction)) {
				result.add(transaction);
			}
			
			if (result.size() >= limit) {
				break;
			}
		}
		
		return result;
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		if (!(functionInput instanceof SlowTransactionsInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender)
	{
		SlowTransactionsInput stInput = (SlowTransactionsInput)input;
	
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(stInput.timeFilter);
		Collection<String> transactions = getSlowTransactions(serviceId, stInput, timespan, stInput.limit);
		
		StringBuilder value = new StringBuilder();
		
		for (String transaction : transactions) {
			value.append(getServiceValue(transaction, serviceId, serviceIds));
			value.append(GRAFANA_SEPERATOR_RAW);
		}
		
		appender.append(value.toString());
	}
	
}
