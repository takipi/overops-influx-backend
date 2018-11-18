package com.takipi.integrations.grafana.settings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Objects;

public class ApplicationGroupSettings {
	
	public static class AppGroup {
		public String name;
		public List<String> applications;
	}
	
	public List<AppGroup> groups;
	
	
	public Collection<String> getApps(String groupName) {
		
		if (groups == null) {
			return Collections.emptyList();
		}
		
		for (AppGroup appGroup : groups) {
			if (Objects.equal(appGroup.name, groupName)) {
				return appGroup.applications;
			}
		}
		
		return Collections.emptyList();
		
	}
}
