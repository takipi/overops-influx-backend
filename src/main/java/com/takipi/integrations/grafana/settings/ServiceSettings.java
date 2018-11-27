package com.takipi.integrations.grafana.settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.infra.Categories.Category;
import com.takipi.common.util.CollectionUtil;

public class ServiceSettings {
	
	private ServiceSettingsData data;
	private boolean initialized;
	private volatile Categories instance = null;
	
	public ServiceSettings(ServiceSettingsData data) {
		this.data = data;
	}
	
	public ServiceSettingsData getData() {
		
		if (data == null) {
			return new ServiceSettingsData();
		}
		
		return data;
	}
	
	public Categories getCategories() {
		
		Categories defaultCategories = Categories.defaultCategories();
		
		if ((data.tiers == null) || (CollectionUtil.safeIsEmpty(data.tiers))) {
			return defaultCategories;
		}
		
		if ((instance == null) && (!initialized)) {
			
			synchronized (Categories.class) {
				if ((instance == null) && (!initialized)) {
					initialized = true;
					instance = new Categories();
					
					instance.categories = new ArrayList<Categories.Category>(data.tiers);
					instance.categories.addAll(defaultCategories.categories);
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
