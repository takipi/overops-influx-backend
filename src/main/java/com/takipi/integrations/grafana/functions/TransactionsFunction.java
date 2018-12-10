package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.transaction.Transaction;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings;
import com.takipi.integrations.grafana.settings.GroupSettings.Group;
import com.takipi.integrations.grafana.util.TimeUtil;

public class TransactionsFunction extends EnvironmentVariableFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new TransactionsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return ViewInput.class;
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
	@SuppressWarnings("unchecked")
	protected int compareValues(Object o1, Object o2) {
		
		Object a = ((List<Object>) o1).get(1);
		Object b = ((List<Object>) o2).get(1);
		
		if (GroupSettings.isGroup(a.toString())) {
			return -1;
		}
		
		if (GroupSettings.isGroup(b.toString())) {
			return 1;
		}
		
		return super.compareValues(o1, o2);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, Collection<String> serviceIds, String serviceId,
			VariableAppender appender) {
		
		ViewInput viewInput = (ViewInput)input;
		Pair<DateTime, DateTime> timespan = TimeUtil.getTimeFilter(viewInput.timeFilter);

		String viewId = getViewId(serviceId, viewInput.view);
		
		if (viewId == null) {
			return;
		}
		
		GroupSettings groupSettings = GrafanaSettings.getData(apiClient, serviceId).transactions;
		
		if (groupSettings != null) {
		
			for (Group group : groupSettings.getGroups()) {
				String groupName = getServiceValue(group.toGroupName(), serviceId, serviceIds);
				appender.append(groupName);		
			}
		}
		
		Collection<Transaction> transactions = getTransactions(serviceId, viewId, timespan, viewInput, null);
		
		if (transactions == null) {
			return;
		}
		
		Map<String, List<String>> transactionMap = new HashMap<String, List<String>>();
		
		for (Transaction transaction : transactions) {
			
			Pair<String, String> nameAndMethod = getTransactionNameAndMethod(transaction.name);
			
			String className = nameAndMethod.getFirst();
			String methodName = nameAndMethod.getSecond();
			
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
