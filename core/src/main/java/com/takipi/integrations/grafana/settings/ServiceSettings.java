package com.takipi.integrations.grafana.settings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import com.takipi.api.client.util.infra.Categories;
import com.takipi.api.client.util.infra.Categories.Category;
import com.takipi.api.client.util.settings.ServiceSettingsData;
import com.takipi.common.util.CollectionUtil;

public class ServiceSettings {

	private final String json;
	private final ServiceSettingsData data;

	private final Categories categories;

	public ServiceSettings(String json, ServiceSettingsData data) {
		this.json = json;
		this.data = data;

		this.categories = initCategories();
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

	private Categories initCategories() {
		if ((data == null) || (CollectionUtil.safeIsEmpty(data.tiers))) {
			return Categories.defaultCategories();
		}

		Categories.fillMissingCategoryNames(data.tiers);

		return Categories.expandWithDefaultCategories(data.tiers);
	}

	public Categories getCategories() {
		return categories;
	}

	public Collection<String> getTierNames() {
		if ((data == null) || (CollectionUtil.safeIsEmpty(data.tiers))) {
			return Collections.emptyList();
		}

		List<String> result = Lists.newArrayList();

		for (Category category : data.tiers) {
			result.addAll(category.labels);
		}

		return result;
	}
}
