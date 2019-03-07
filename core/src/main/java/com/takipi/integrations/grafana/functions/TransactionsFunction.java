package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.Transaction;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.BaseEventVolumeInput;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.TransactionsInput;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings;
import com.takipi.integrations.grafana.settings.GroupSettings.Group;
import com.takipi.integrations.grafana.settings.GroupSettings.GroupFilter;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TransactionsFunction extends EnvironmentVariableFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return TransactionsInput.class;
		}

		@Override
		public String getName() {
			return "transactions";
		}
	}

	public TransactionsFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	@Override
	protected int compareValues(FunctionInput input, String o1, String o2) {
		
		int i1 = GrafanaFunction.TOP_TRANSACTION_FILTERS.indexOf(o1);
		int i2 = GrafanaFunction.TOP_TRANSACTION_FILTERS.indexOf(o2);
	
		if (i2 != -1) {
			if (i1 != -1) {
				return i2 - i1;
			} else {
				return 1;
			}
		} else {
			if (i1 != -1) {
				return -1;
			}
		}
		
		if (GroupSettings.isGroup(o1.toString())) {
			return -1;
		}
		
		if (GroupSettings.isGroup(o2.toString())) {
			return 1;
		}
		
		return super.compareValues(input, o1, o2);
	}
	
	@Override
	protected void populateValues(FunctionInput input, VariableAppender appender)
	{
		for (String topTransactionFilter : GrafanaFunction.TOP_TRANSACTION_FILTERS) {
			appender.append(topTransactionFilter);	
		}
		
		super.populateValues(input, appender);
	}

	private BaseEventVolumeInput getInput(BaseEnvironmentsInput input) {
		
		BaseEventVolumeInput beInput = (BaseEventVolumeInput)input;
		
		Gson gson = new Gson();
		String json = gson.toJson(input);
		
		BaseEventVolumeInput result = gson.fromJson(json, beInput.getClass());
		
		if (beInput.timeFilter != null) {
			Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(beInput.timeFilter);
			result.timeFilter = TimeUtil.getTimeFilter(timespan);	
		} 
		
		return result;
		
	}
	
	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {
		
		BaseEventVolumeInput viewInput = getInput(input);
		
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(viewInput.timeFilter);

		String viewId = getViewId(serviceId, viewInput.view);
		
		if (viewId == null) {
			return;
		}
		
		Collection<Transaction> transactions = getTransactions(serviceId, viewId, timespan, 
			viewInput, null);
		
		if (transactions == null) {
			return;
		}
				
		GroupSettings groupSettings = GrafanaSettings.getData(apiClient, serviceId).transactions;
		
		Set<Group> matchingGroups = new HashSet<Group>();	
		Map<Group, GroupFilter> groupFilters;
		
		if (groupSettings != null) {
						
			groupFilters = new HashMap<Group, GroupFilter>();

			for (Group group : groupSettings.getGroups()) {
				
				GroupFilter filter = group.getFilter();
				groupFilters.put(group, filter);		
			}
		} else {
			groupFilters = null;
		}
		
		Map<String, List<String>> transactionMap = new HashMap<String, List<String>>();
		
		for (Transaction transaction : transactions) {
			
			Pair<String, String> nameAndMethod = getTransactionNameAndMethod(transaction.name, false);
			
			String className = nameAndMethod.getFirst();
			String methodName = nameAndMethod.getSecond();
			
			if (groupFilters != null) {
				
				Pair<String, String> fullNameAndMethod = getFullNameAndMethod(transaction.name);
				
				for (Map.Entry<Group, GroupFilter> entry : groupFilters.entrySet()) {
					
					if (!filterTransaction(entry.getValue(), null, fullNameAndMethod.getFirst(), fullNameAndMethod.getSecond())) {
						matchingGroups.add(entry.getKey());
					}
				}
			}
			
			List<String> transactionMethods = transactionMap.get(className);

			if (methodName != null) {
				if (transactionMethods == null) {
					transactionMethods = new ArrayList<String>();
					transactionMap.put(className, transactionMethods);
				}
				transactionMethods.add(methodName);

			} else {
				if (!transactionMap.containsKey(className)) {
					transactionMap.put(className, null);
				}
			}
		}
		
		for (Group group : matchingGroups) {
			String groupName = getServiceValue(group.toGroupName(), serviceId, serviceIds);
			appender.append(groupName);	
		}
		
		for (Map.Entry<String, List<String>> entry : transactionMap.entrySet()) {
			
			String className = entry.getKey();
			List<String> methodNames = entry.getValue();
			
			if ((methodNames != null) && (methodNames.size() > 1)) {
				for (String method : methodNames) {
					String serviceTransaction = getServiceValue(className + QUALIFIED_DELIM + method, serviceId, serviceIds);
					appender.append(serviceTransaction);	
				}
			} else {
				String serviceTransaction = getServiceValue(className, serviceId, serviceIds);
				appender.append(serviceTransaction);	
			}					
		}
	}
}
