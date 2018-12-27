package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.Transaction;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.BaseEnvironmentsInput;
import com.takipi.integrations.grafana.input.TransactionsInput;
import com.takipi.integrations.grafana.input.ViewInput;
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
	protected int compareValues(String o1, String o2) {
		
		if (GroupSettings.isGroup(o1.toString())) {
			return -1;
		}
		
		if (GroupSettings.isGroup(o2.toString())) {
			return 1;
		}
		
		return super.compareValues(o1, o2);
	}

	@Override
	protected void populateServiceValues(BaseEnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {
		
		ViewInput viewInput = (ViewInput)input;
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(viewInput.timeFilter);

		String viewId = getViewId(serviceId, viewInput.view);
		
		if (viewId == null) {
			return;
		}
		
		Collection<Transaction> transactions = getTransactions(serviceId, viewId, timespan, viewInput, null);
		
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
			
			Pair<String, String> nameAndMethod = getTransactionNameAndMethod(transaction.name);
			
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
