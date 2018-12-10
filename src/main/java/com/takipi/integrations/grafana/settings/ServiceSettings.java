package com.takipi.integrations.grafana.settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.infra.Categories.Category;
import com.takipi.common.util.CollectionUtil;
import com.takipi.integrations.grafana.settings.GroupSettings.GroupFilter;

public class ServiceSettings {
	
	private ServiceSettingsData data;
	private boolean initialized;
	private volatile Categories instance = null;
	private String serviceId;
	private ApiClient apiClient;
	
	public ServiceSettings(String serviceId, ApiClient apiClient, ServiceSettingsData data) {
		this.data = data;
		this.serviceId = serviceId;
		this.apiClient = apiClient;
	}
	
	public ServiceSettingsData getData() {
		
		if (data == null) {
			return new ServiceSettingsData();
		}
		
		return data;
	}
	
	public GroupFilter getTransactionsFilter(Collection<String> transactions) {
		
		GroupFilter result;
		
		if (transactions != null)
		{
			
			GroupSettings transactionGroups = GrafanaSettings.getData(apiClient, serviceId).transactions;
			
			if (transactionGroups != null)
			{
				result = transactionGroups.getExpandedFilter(transactions);
			}
			else
			{
				result = GroupFilter.from(transactions);
			}
		}
		else
		{
			result = null;
		}
		
		return result;
	}
	
	public Categories getCategories() {
		
		Categories defaultCategories = Categories.defaultCategories();
		
		if ((data.tiers == null) || (CollectionUtil.safeIsEmpty(data.tiers))) {
			return defaultCategories;
		}
		
		if ((instance == null) && (!initialized)) {
			
			synchronized (Categories.class) {
				if ((instance == null) && (!initialized)) {
					instance = new Categories();
					instance.categories = new ArrayList<Categories.Category>(data.tiers);
					instance.categories.addAll(defaultCategories.categories);
					initialized = true;
				}
			}
		}
		
		return instance;
	}
	
	public Collection<String> getTierNames() {
		
		if (data.tiers == null) {
			return Collections.emptyList();
		}
		
		 List<String> result = new ArrayList<>();
		
		for (Category category : data.tiers) {
			result.addAll(category.labels);
		}
		
		return result;
	}
}
