package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.deployment.SummarizedDeployment;
import com.takipi.api.client.data.event.Stats;
import com.takipi.api.client.result.deployment.DeploymentsResult;
import com.takipi.api.client.result.event.EventResult;
import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.CollectionUtil;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EventGroupInput;
import com.takipi.integrations.grafana.input.EventGroupInput.GroupType;
import com.takipi.integrations.grafana.input.EventsInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.output.Series;
import com.takipi.integrations.grafana.util.ApiCache;
import com.takipi.integrations.grafana.util.TimeUtil;
import com.takipi.integrations.grafana.util.ApiCache.BreakdownType;
import com.takipi.integrations.grafana.util.DeploymentUtil;

public class EventGroupFunction extends EventsFunction
{
	public EventGroupFunction(ApiClient apiClient) {
		super(apiClient);
	}

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new EventGroupFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EventGroupInput.class;
		}

		@Override
		public String getName() {
			return "eventGroup";
		}
	}
	
	protected class GroupData {
		
		protected String name;
		protected Set<String> apps;
		protected Set<String> deps;
		protected Set<String> servers;
		protected Map<String, EventResult> events;
		protected long volume;
		protected Collection<EventData> mergedEvents;
		protected String firstSeen;
		protected String lastSeen;
		
		protected GroupData(String name) {
			this.name = name;
			this.apps = new TreeSet<>();
			this.deps = new TreeSet<>();
			this.servers = new TreeSet<>();
			this.events = new HashMap<String, EventResult>();
		}
	}
	
	protected class EventKeyVolume {
		protected long volume;
		protected String key;
	}
	
	private static final Set<BreakdownType> APP_TYPES = 
		new HashSet<>(Arrays.asList(new BreakdownType[] {
				BreakdownType.App, BreakdownType.Deployment}));
	
	private static final Set<BreakdownType> DEP_TYPES = 
		new HashSet<>(Arrays.asList(new BreakdownType[] {
				BreakdownType.Deployment, BreakdownType.App}));			
	
	private static final Set<BreakdownType> SRV_TYPES = 
		new HashSet<>(Arrays.asList(new BreakdownType[] {
			BreakdownType.Server, BreakdownType.App}));
	
	private Set<BreakdownType> getBreakdownTypes(GroupType groupType) {
		
		switch (groupType) {
			
			case Applications:
				return APP_TYPES;
			case Deployment:
				return DEP_TYPES;
			case ServerGroup:
				return SRV_TYPES;
		
			default:
				throw new IllegalStateException(String.valueOf(groupType));
			
		}
	}
		
	private Map<String, Collection<GroupData>> getEventGroupMap(EventGroupInput input, 
		Collection<String> serviceIds, GroupType groupType, Pair<DateTime, DateTime> timespan) {
				
		Map<String, Collection<GroupData>> result = new TreeMap<String, Collection<GroupData>>();
		
		for (String serviceId : serviceIds) {
			
			Map<String, GroupData> serviceGroups = new HashMap<String, GroupData>();
			result.put(serviceId, serviceGroups.values());
			
			EventFilter eventFilter = getEventFilter(serviceId, input, timespan);
			
			if (eventFilter == null) {
				continue;
			}
			
			Map<String, EventResult> eventMap = getEventMap(serviceId, input, timespan.getFirst(), timespan.getSecond(), 
				VolumeType.hits, false, getBreakdownTypes(groupType));
			
			if (eventMap == null) {
				continue;
			}
			
			for (EventResult event : eventMap.values()) {
			
				if (eventFilter.filter(event)) {
					continue;
				}
				
				if (CollectionUtil.safeIsEmpty(event.stats.contributors)) {
					continue;
				}
					
				for (Stats stats : event.stats.contributors) {
					
					String key;
					
					switch (groupType) {
						case Applications:
							key = stats.application_name;
							break;
						case Deployment:
							key = stats.deployment_name;
							break;						
						case ServerGroup:
							key = stats.machine_name;
							break;	
						default:
							throw new IllegalStateException(String.valueOf(groupType));	
					}
					
					if (key == null) {
						continue;
					}
										
					GroupData groupData = serviceGroups.get(key);
					
					if (groupData == null) {
						groupData = new GroupData(key);
						serviceGroups.put(key, groupData);
					}
					
					if (stats.application_name != null) {
						groupData.apps.add(stats.application_name);
					}
					
					if (stats.deployment_name != null) {
						groupData.deps.add(stats.deployment_name);
					}	
				
					if (stats.machine_name != null) {
						groupData.servers.add(stats.machine_name);
					}
					
					groupData.events.put(event.id, event);
					groupData.volume += stats.hits;
				}
			}
			
			for (GroupData groupData : serviceGroups.values()) {
				groupData.mergedEvents = groupEvents(serviceId, groupData.events.values());
			}
		}
		
		return result;
	}
	
	private String formatKeys(Collection<String> keys) {
		return String.join(ARRAY_SEPERATOR_RAW + " ", keys);
	}
	
	private void setRow(Object[] object, List<String> columns, String column, Object value) {
		
		int index = columns.indexOf(column);
		
		if (index == -1) {
			throw new IllegalStateException(String.join(ARRAY_SEPERATOR_RAW, columns) + ": " + column + " = " + value);
		}
		
		object[index] = value;
	}
	
	private Object[] getAppObject(String key, GroupData groupData) {
		
		List<String> fields = EventGroupInput.APP_FIELDS;
		Object[] result = new Object[fields.size()];
		
		setRow(result, fields, EventGroupInput.APPLICATION, key);
		setRow(result, fields, EventGroupInput.EVENTS, groupData.volume);
		setRow(result, fields, EventGroupInput.EVENT_LOCATIONS, groupData.mergedEvents.size());
		setRow(result, fields, EventGroupInput.DEPLOYMENTS, formatKeys(groupData.deps));

		return result;	
	}
	
	private Object[] getServerObject(String key, GroupData groupData) {
		
		List<String> fields = EventGroupInput.SERVER_FIELDS;
		Object[] result = new Object[fields.size()];
		
		setRow(result, fields, EventGroupInput.SERVER_GROUP, key);
		setRow(result, fields, EventGroupInput.EVENTS, groupData.volume);
		setRow(result, fields, EventGroupInput.EVENT_LOCATIONS, groupData.mergedEvents.size());
		setRow(result, fields, EventGroupInput.APPLICATIONS, formatKeys(groupData.apps));

		return result;	
	}
	
	private Object[] getDepObject(String serviceId, String key, GroupData groupData,
		Pair<DateTime, DateTime> timespan, Map<String, String> introByGroupMap) {
		
		List<String> fields = EventGroupInput.DEP_FIELDS;
		Object[] result = new Object[fields.size()];
		
		List<EventData> introducedBy = new ArrayList<EventData>();
		
		for (EventResult event : groupData.events.values()) {
			
			if (event.first_seen == null) {
				continue;
			}
			
			DateTime firstSeen = TimeUtil.getDateTime(event.first_seen);
			
			if (firstSeen.isBefore(timespan.getFirst())) {
				continue;
			}
			
			String depName = introByGroupMap.get(event.id);
			
			if (!groupData.name.equals(depName)) {
				continue;
			}
			
			if (groupData.name.equals(event.introduced_by)) {
				introducedBy.add(new EventData(event));
			}
		}
		
		System.out.print(groupData.name + " : ");
		
		List<EventData> mergedIntroBy = mergeSimilarEvents(serviceId, false, introducedBy);
		
		for (EventData merged : mergedIntroBy) {
			System.out.print(merged.event.id + ",");
		}
		System.out.println();
		
		setRow(result, fields, EventGroupInput.DEPLOYMENT, key);
		setRow(result, fields, EventGroupInput.EVENTS, groupData.volume);
		setRow(result, fields, EventGroupInput.EVENT_LOCATIONS, groupData.mergedEvents.size());
		setRow(result, fields, EventGroupInput.NEW_EVENTS, mergedIntroBy.size());
		setRow(result, fields, EventGroupInput.FIRST_EVENT, TimeUtil.getEpoch(groupData.firstSeen));
		setRow(result, fields, EventGroupInput.LAST_EVENT, TimeUtil.getEpoch(groupData.lastSeen));	
		setRow(result, fields, EventGroupInput.APPLICATIONS, formatKeys(groupData.apps));

		return result;	
	}
	
	private Collection<EventData> groupEvents(String serviceId, Collection<EventResult> events) {
		
		List<EventData> eventDatas = new ArrayList<EventData>(events.size());
		
		for (EventResult event : events) {
			eventDatas.add(new EventData(event));
		}
		
		List<EventData> result = mergeSimilarEvents(serviceId, false, eventDatas);
		
		return result;
	}
	
	private void updateDeploymentSeen(String serviceId, EventsInput input, Collection<GroupData> groupDatas) {
		
		Response<DeploymentsResult> response = ApiCache.getDeployments(apiClient, serviceId, false, input.query);
		
		if ((response.isBadResponse()) || (response.data == null) 
		|| (CollectionUtil.safeIsEmpty(response.data.deployments))) {
			return;
		}
				
		for (SummarizedDeployment dep : response.data.deployments) {
			
			for (GroupData groupData : groupDatas) {
				
				if (!groupData.name.equals(dep.name)) {
					continue;
				}
				
				groupData.firstSeen = dep.first_seen;
				groupData.lastSeen = dep.last_seen;
			}	
		}
	}
	
	private List<GroupData> sortGroupDatas(GroupType groupType,
		Collection<GroupData> groupDatas) {
		
		List<GroupData> result = new ArrayList<GroupData>(groupDatas);
		
		result.sort(new Comparator<GroupData>() {

			@Override
			public int compare(GroupData o1, GroupData o2) {
				
				if (groupType == GroupType.Deployment) {
					
					if ((o1.lastSeen != null) && (o2.lastSeen != null)) {
						return Long.compare(
							TimeUtil.getEpoch(o2.lastSeen), 
							TimeUtil.getEpoch(o1.lastSeen));
					} 
					
					return DeploymentUtil.compareDeployments(o1.name, o2.name);
				}
				
				return o1.name.compareTo(o2.name);
			}
		});
		
		return result;
		
	}
	
	private Map<String, String> getIntroByGroupMap(String serviceId, Collection<GroupData> groupDatas) {
		
		Map<String, String> result = new HashMap<String, String>();
		
		List<EventData> introBy = new ArrayList<EventData>();
		
		for (GroupData groupData : groupDatas) {
			for (EventResult event : groupData.events.values()) {
				introBy.add(new EventData(event));
			}
		}
		
		List<EventData> mergedIntroBy = mergeSimilarEvents(serviceId, false, introBy);
		
		for (EventData eventData : mergedIntroBy) {
			result.put(eventData.event.id, eventData.event.introduced_by);
		}
		
		return result;
	}
	
	private List<Series> getGroupSeries(EventGroupInput input, GroupType groupType,
		List<String> columns) {
		
		List<List<Object>> values = new ArrayList<List<Object>>();
		Collection<String> serviceIds = input.getServiceIds();

		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(input.timeFilter);

		Map<String, Collection<GroupData>> groupsMap = getEventGroupMap(input, 
			serviceIds, groupType, timespan);

		Map<String, String> introByGroupMap;
		
		for (Map.Entry<String, Collection<GroupData>> serviceEntry : groupsMap.entrySet()) {
		
			String serviceId = serviceEntry.getKey();
			Collection<GroupData> groupDatas = serviceEntry.getValue(); 
			
			if (groupType == GroupType.Deployment) {
				updateDeploymentSeen(serviceId, input, groupDatas);
				
				introByGroupMap = getIntroByGroupMap(serviceId, groupDatas);
			} else {
				introByGroupMap = null;
			}
			
			List<GroupData> sorted = sortGroupDatas(groupType, groupDatas);
			
			for (GroupData groupData : sorted) {
				
				Object[] groupObject;
				String groupKey = getServiceValue(groupData.name, serviceId, serviceIds);
				
				switch (groupType) {
					
					case Applications:
						groupObject = getAppObject(groupKey, groupData);
						break;
					
					case Deployment:
						groupObject = getDepObject(serviceId, groupKey, 
							groupData, timespan, introByGroupMap);
						break;
						
					case ServerGroup:
						groupObject = getServerObject(groupKey, groupData);
						break;
					
					default:
						throw new IllegalStateException(String.valueOf(groupType));
					
				}
				
				if (groupObject != null) {
					values.add(Arrays.asList(groupObject));
				}
			}		
		}
		
		return Collections.singletonList(createSeries(values, columns));
	}
	
	private List<Series> getEventSeries(EventGroupInput input, GroupType groupType) {
		
		EventGroupInput eventInput;
		
		if (groupType == GroupType.CodeLocation) {
			eventInput = input;
		} else {
			Gson gson = new Gson();
			String json = gson.toJson(input);
			eventInput = gson.fromJson(json, input.getClass());
			eventInput.skipGrouping = true;
		}
		
		return super.process(eventInput);
	}
	
	@Override
	public List<Series> process(FunctionInput functionInput) {
		
		if (!(functionInput instanceof EventGroupInput)) {
			throw new IllegalArgumentException("functionInput");
		}
		
		EventGroupInput input = (EventGroupInput)functionInput;
		
		GroupType groupType = input.getGroupType();
		
		switch (groupType) {
			case None:
			case CodeLocation:
				return getEventSeries(input, groupType);
			case Applications:
				return getGroupSeries(input, groupType, EventGroupInput.APP_FIELDS);
			case Deployment:
				return getGroupSeries(input, groupType, EventGroupInput.DEP_FIELDS);
			case ServerGroup:
				return getGroupSeries(input, groupType, EventGroupInput.SERVER_FIELDS);
			default:
				throw new IllegalStateException(String.valueOf(groupType));	
		}
	}
}
