package com.takipi.integrations.grafana.settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.infra.Categories.Category;
import com.takipi.common.util.CollectionUtil;
import com.takipi.integrations.grafana.settings.GroupSettings.GroupFilter;
import com.takipi.integrations.grafana.settings.input.ServiceSettingsData;

public class ServiceSettings {
	
	private ServiceSettingsData data;
	private volatile Categories instance = null;
	private String serviceId;
	private ApiClient apiClient;
	private String json;
	
	public ServiceSettings(String serviceId, ApiClient apiClient, String json, ServiceSettingsData data) {
		this.data = data;
		this.serviceId = serviceId;
		this.apiClient = apiClient;
		this.json = json;
	}
	
	public String getJson() {
		return json;
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
	
	private boolean isMatch(Category a, Category b) {
		
		if ((a.labels == null) || (b.labels == null)) {
			return false;
		}
		
		for (String labelA : a.labels) {
			for (String labelB : b.labels) {
				if (Objects.equals(labelA ,labelB)) {
					return true;
				}
			}
		}
		
		return false;
	}
	
	public Categories getCategories() {
		
		Categories defaultCategories = Categories.defaultCategories();
		
		if (CollectionUtil.safeIsEmpty(data.tiers)) {
			return defaultCategories;
		}
		
		if (instance == null) {
			synchronized (Categories.class) {
				if (instance == null) {
					
					for (Category tier : data.tiers) {
						if (CollectionUtil.safeIsEmpty(tier.names)) {
							for (Category defaultCategory : defaultCategories.categories) {
								if (isMatch(tier, defaultCategory)) {
									tier.names = Lists.newArrayList(defaultCategory.names);
									break;
								}
							}
						}
					}
					
					Categories newInstance = new Categories();
					newInstance.categories = Lists.newArrayList(data.tiers);
					
					newInstance.categories.addAll(defaultCategories.categories);
					
					instance = newInstance;
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
