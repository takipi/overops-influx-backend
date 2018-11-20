package com.takipi.integrations.grafana.settings;

import java.util.ArrayList;

import com.takipi.api.client.util.infra.Categories;
import com.takipi.common.util.CollectionUtil;

public class ServiceSettings {
	
	public EventSettings gridSettings;
	public GraphSettings graphSettings;
	public TransactionSettings transactions;
	public ApplicationGroupSettings applicationGroups;
	public CategorySettings categories;
	public RegressionSettings regressionSettings;
	public RegressionReportSettings regressionReport;
	
	private boolean initialized;
	private volatile Categories instance = null;
	
	public Categories getCategories() {
		
		Categories defaultCategories = Categories.defaultCategories();
		
		if ((categories == null) || (CollectionUtil.safeIsEmpty(categories.categories))) {
			return defaultCategories;
		}
		
		if ((instance == null) && (!initialized)) {
			
			synchronized (Categories.class) {
				if ((instance == null) && (!initialized)) {
					initialized = true;
					instance = new Categories();
					
					instance.categories = new ArrayList<Categories.Category>(categories.categories);
					instance.categories.addAll(defaultCategories.categories);
				}
			}
		}
		
		return instance;
	}
}
