package com.takipi.integrations.grafana.settings;

import java.util.List;

import com.takipi.api.client.util.infra.Categories.Category;

public class ServiceSettingsData
{
	public GeneralSettings general;
	
	public GroupSettings transactions;
	public GroupSettings applications;
	public List<Category> tiers;	
	
	public UserGroupsSettings user_groups;
	
	public SlowdownSettings slowdown;
	public RegressionSettings regression;
	
	public RegressionReportSettings regression_report;
}
