package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.categories.Categories;
import com.takipi.integrations.grafana.functions.EventFilter;
import com.takipi.integrations.grafana.functions.TransactionsFunction;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.TransactionSettings;

public class EventFilterInput extends ViewInput {

	public String introducedBy;
	public String types;
	public String labels;
	public String labelsRegex;
	public String firstSeen;
	public int pointsWanted;

	public boolean hasEventFilter() {
		
		if ((introducedBy != null) && (introducedBy.length() > 0)) {
			return true;
		}
		
		if ((types != null) && (types.length() > 0)) {
			return true;
		}
		
		if ((labels != null) && (labels.length() > 0)) {
			return true;
		}
		
		if ((labelsRegex != null) && (labelsRegex.length() > 0)) {
			return true;
		}
		
		if ((firstSeen != null) && (firstSeen.length() > 0)) {
			return true;
		}
		
		if ((transactions != null) && (transactions.length() > 0)) {
			return true;
		}
		
		return false;
	}
	
	public boolean hasIntroducedBy() {
		return hasFilter(introducedBy);
	}

	public Collection<String> getIntroducedBy(String serviceId) {

		if (introducedBy == null) {
			return Collections.emptySet();
		}

		return getServiceFilters(introducedBy, serviceId, true);
	}

	public Collection<String> getTypes() {

		if (!hasFilter(types)) {
			return null;
		}

		return getServiceFilters(types, null, true);
	}

	public Collection<String> geLabels(String serviceId) {

		if (labels == null) {
			return Collections.emptySet();
		}

		return getServiceFilters(labels, serviceId, true);
	}

	public EventFilter getEventFilter(ApiClient apiClient, String serviceId) {
		
		Categories categories = GrafanaSettings.getServiceSettings(apiClient, serviceId).getCategories();
		
		Collection<String> transactions = getTransactions(serviceId);
		
		if ((transactions != null) && transactions.contains(TransactionsFunction.KEY_TRANSACTIONS)) {
			
			List<String> expandedTx = new ArrayList<String>(transactions);
			expandedTx.remove(TransactionsFunction.KEY_TRANSACTIONS);
			
			TransactionSettings txSettings = GrafanaSettings.getServiceSettings(apiClient, serviceId).transactions;
			
			if ((txSettings != null) && (txSettings.keyTransactions != null)) {
				expandedTx.addAll(txSettings.getKeyTransactions());
			}
			
			transactions = expandedTx;		
		}
		
		
		return EventFilter.of(getTypes(), getIntroducedBy(serviceId), transactions, geLabels(serviceId),
				labelsRegex, firstSeen, categories);
	}

}
