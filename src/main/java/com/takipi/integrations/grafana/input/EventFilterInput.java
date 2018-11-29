package com.takipi.integrations.grafana.input;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.integrations.grafana.functions.EventFilter;
import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.settings.GeneralSettings;
import com.takipi.integrations.grafana.settings.GrafanaSettings;
import com.takipi.integrations.grafana.settings.GroupSettings.GroupFilter;

public class EventFilterInput extends ViewInput
{
	
	public String introducedBy;
	public String types;
	public String labels;
	public String labelsRegex;
	public String firstSeen;
	public int pointsWanted;
	public String searchText;
	public String allowedTypes;
	
	public boolean hasEventFilter()
	{
		
		if ((introducedBy != null) && (introducedBy.length() > 0))
		{
			return true;
		}
		
		if ((types != null) && (types.length() > 0))
		{
			return true;
		}
		
		if ((labels != null) && (labels.length() > 0))
		{
			return true;
		}
		
		if ((labelsRegex != null) && (labelsRegex.length() > 0))
		{
			return true;
		}
		
		if ((firstSeen != null) && (firstSeen.length() > 0))
		{
			return true;
		}
		
		if ((transactions != null) && (transactions.length() > 0))
		{
			return true;
		}
		
		return false;
	}
	
	public boolean hasIntroducedBy()
	{
		return hasFilter(introducedBy);
	}
	
	public Collection<String> getIntroducedBy(String serviceId)
	{
		
		if (introducedBy == null)
		{
			return Collections.emptySet();
		}
		
		return getServiceFilters(introducedBy, serviceId, true);
	}
	
	public Collection<String> getTypes()
	{
		
		if (!hasFilter(types))
		{
			return null;
		}
		
		return getServiceFilters(types, null, true);
	}
	
	public static Collection<String> toArray(String value)
	{
		if (value == null)
		{
			return Collections.emptyList();
		}
		
		String[] types = value.split(GrafanaFunction.ARRAY_SEPERATOR);
		return Arrays.asList(types);
		
	}
	
	public Collection<String> geLabels(String serviceId)
	{
		
		if (labels == null)
		{
			return Collections.emptySet();
		}
		
		return getServiceFilters(labels, serviceId, true);
	}
	
	public EventFilter getEventFilter(ApiClient apiClient, String serviceId)
	{
		
		Categories categories = GrafanaSettings.getServiceSettings(apiClient, serviceId).getCategories();
		
		Collection<String> transactions = getTransactions(serviceId);
		GroupFilter transactionsFilter = GrafanaSettings.getServiceSettings(apiClient, serviceId).getTransactionsFilter(transactions);

		Collection<String> allowedTypes;
		
		if (this.allowedTypes != null)
		{
			allowedTypes = toArray(this.allowedTypes);
		}
		else
		{
			GeneralSettings generalSettings = GrafanaSettings.getData(apiClient, serviceId).general;
			
			if (generalSettings != null)
			{
				allowedTypes = generalSettings.getDefaultTypes();
			}
			else
			{
				allowedTypes = Collections.emptyList();
			}
		}
		
		return EventFilter.of(getTypes(), allowedTypes, getIntroducedBy(serviceId), transactionsFilter,
				geLabels(serviceId), labelsRegex, firstSeen, categories, searchText);
	}
	
}
