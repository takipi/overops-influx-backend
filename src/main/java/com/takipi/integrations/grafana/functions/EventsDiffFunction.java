package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.regression.RegressionStringUtil;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventsDiffInput;
import com.takipi.integrations.grafana.input.EventsDiffInput.DiffType;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.TimeUtil;

public class EventsDiffFunction extends EventsFunction
{
	public final static String NO_DIFF = "0m";

	public EventsDiffFunction(ApiClient apiClient)
	{
		super(apiClient);
	}
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new EventsDiffFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EventsDiffInput.class;
		}

		@Override
		public String getName() {
			return "eventsDiff";
		}
	}
	
	protected class DiffEventData extends EventData {

		protected EventResult source;
		
		protected DiffEventData(EventResult event, EventResult source)
		{
			super(event);
			this.source = source;
		}
		
		private double getRate(EventResult event) {
			
			if (event.stats.invocations > 0) {
				return (double)(event.stats.hits) / (double)event.stats.invocations;
			} 
			
			return 0;
		}
		
		protected double getDelta() {
			double sourceRate = getRate(source);
			double targetRate = getRate(event);
			
			return targetRate - sourceRate;
		}
		
		protected String getDeltaDesc() {
			return RegressionStringUtil.getEventRate(event, true) + " from " 
				+ RegressionStringUtil.getEventRate(source, true);
		}
	}
	
	protected class DiffDescFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			if (eventData instanceof DiffEventData) {
				DiffEventData diffEventData = (DiffEventData)eventData;
				return diffEventData.getDeltaDesc();
			} else {
				return "New";
			}
		}
		
		@Override
		protected Object formatValue(Object value, EventsInput input)
		{
			return value;
		}
	}
	
	protected class DiffFormatter extends FieldFormatter {

		@Override
		protected Object getValue(EventData eventData, String serviceId, EventsInput input,
				Pair<DateTime, DateTime> timeSpan) {

			if (eventData instanceof DiffEventData) {
				DiffEventData diffEventData = (DiffEventData)eventData;
				
				double delta = diffEventData.getDelta();
				
				if (delta > 10) {
					return ">1000%";
				}
				
				return delta;
			} else {
				return "New";
			}
		}
	}
	
	private Map<String, EventData> getEventDataMap(Collection<EventData> eventDatas) {
		
		 Map<String, EventData> result = new HashMap<String, EventData>();
		 
		 for (EventData eventData : eventDatas) {
			 result.put(eventData.event.id, eventData);
		 }
		 
		 return result;
	}
		
	@Override
	protected List<EventData> getEventData(String serviceId, EventsInput input, Pair<DateTime, DateTime> timeSpan)
	{
		EventsDiffInput eventsDiffInput = (EventsDiffInput)input;
		
		String json = new Gson().toJson(input);
		EventsInput targetInput =  new Gson().fromJson(json, input.getClass());
				
		targetInput.applications = eventsDiffInput.compareToApplications;
		targetInput.servers = eventsDiffInput.compareToServers;
		targetInput.deployments = eventsDiffInput.compareToDeployments;
		
		String sourceTimeFilter;
		Pair<DateTime, DateTime> sourceTimespan;

		if ((eventsDiffInput.timeDiff != null) 
		&& (!eventsDiffInput.timeDiff.equals(NO_DIFF))) {
			
			int offset;
			
			if (NONE.equals(eventsDiffInput.timeDiff)) {
				offset = 0;	
			} else {
				offset = TimeUtil.parseInterval(eventsDiffInput.timeDiff);
			}
			
			sourceTimespan = Pair.of(timeSpan.getFirst().minusMinutes(offset),
				timeSpan.getSecond().minusMinutes(offset));
			sourceTimeFilter = TimeUtil.toTimeFilter(sourceTimespan.getFirst(), sourceTimespan.getSecond());
		} else {
			sourceTimeFilter = null;
			sourceTimespan = timeSpan;
		}
		
		
		if (sourceTimeFilter != null) {
			eventsDiffInput.timeFilter = sourceTimeFilter;
		}
		
		if ((Objects.equal(input.getApplications(apiClient, serviceId), targetInput.getApplications(apiClient, serviceId))) 
		&& (Objects.equal(input.getDeployments(serviceId), targetInput.getDeployments(serviceId))) 
		&& (Objects.equal(input.getServers(serviceId), targetInput.getServers(serviceId)))
		&& (sourceTimeFilter == null)) {
			return Collections.emptyList();
		}
			
		Collection<DiffType> diffTypes = eventsDiffInput.getDiffTypes();
		
		List<EventData> sourceEventDatas = super.getEventData(serviceId, input, sourceTimespan);
		List<EventData> targetEventDatas = super.getEventData(serviceId, targetInput, timeSpan);
		
		Map<String, EventData> sourceEventDataMap = getEventDataMap(sourceEventDatas);
		
		List<EventData> result = new ArrayList<EventData>();
		
		for (EventData targetEventData : targetEventDatas) {
			
			EventData sourceEventData = sourceEventDataMap.get(targetEventData.event.id); 
			
			if (sourceEventData != null) {
				
				if (!diffTypes.contains(DiffType.Increasing)) {
					continue;
				}
				
				DiffEventData diffEventData = new DiffEventData(targetEventData.event, 
						sourceEventData.event);
				
				if (diffEventData.getDelta() > 0f) {
					result.add(diffEventData);
				}
			} else {
				
				if (!diffTypes.contains(DiffType.New)) {
					continue;
				}
				
				boolean foundSimiliarId = false;
				
				if (!CollectionUtil.safeIsEmpty(targetEventData.event.similar_event_ids)) {
						
					for (String Id : targetEventData.event.similar_event_ids) {
						if (sourceEventDataMap.containsKey(Id)) {
							foundSimiliarId = true;
							break;
						}
					}
				}
				
				if (!foundSimiliarId) {
					result.add(targetEventData);
				}
			}
		}
		
		if ((eventsDiffInput.limit == null) || (eventsDiffInput.limit.equals(ALL))) {
			return result;		
		}
		
		int limit = Integer.valueOf(eventsDiffInput.limit);
		return result.subList(0, Math.min(result.size(), limit));
	}	
		
	@Override
	protected List<EventData> mergeEventDatas(List<EventData> eventDatas)
	{
		for (EventData eventData : eventDatas) {
			if (eventData instanceof DiffEventData) {
				return eventDatas;
			}
			
		}
		
		return super.mergeEventDatas(eventDatas);
	}
	
	@Override
	protected FieldFormatter getFormatter(String serviceId, String column) {
		
		if (column.equals(EventsDiffInput.DIFF)) {
			return new DiffFormatter();
		}
		
		if (column.equals(EventsDiffInput.DIFF_DESC)) {
			return new DiffDescFormatter();
		}
		
		return super.getFormatter(serviceId, column);
	}
	
	@Override
	protected void sortEventDatas(List<EventData> eventDatas)
	{
		eventDatas.sort(new Comparator<EventData>() {
			
			@Override
			public int compare(EventData o1, EventData o2)
			{
				DiffEventData d1;
				DiffEventData d2;
				
				if (o1 instanceof DiffEventData) {
					d1 = (DiffEventData)o1;
				} else {
					d1 = null;
				}
				
				if (o2 instanceof DiffEventData) {
					d2 = (DiffEventData)o2;
				} else {
					d2 = null;
				}
				
				if ((d1 != null) && (d2 != null)) {
					if (d2.getDelta() > d1.getDelta()) {
						return 1;
					} else {
						return -1;
					}
				}
				
				if ((d1 == null) && (d2 == null)) {
					return(int)(o2.event.stats.hits - o1.event.stats.hits);
				}
				
				if ((d1 != null) && (d2 == null)) {
					return 1;		
				}
				
				if ((d1 == null) && (d2 != null)) {
					return -1;		
				}
				
				throw new IllegalStateException();
			}
		});
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof EventsDiffInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		return super.process(functionInput);
	}
	
}
