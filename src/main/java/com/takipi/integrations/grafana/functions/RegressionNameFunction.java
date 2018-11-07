package com.takipi.integrations.grafana.functions;

import java.util.Collection;
import java.util.Date;

import org.joda.time.DateTime;
import org.ocpsoft.prettytime.PrettyTime;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.regression.RegressionInput;
import com.takipi.api.client.util.regression.RegressionStringUtil;
import com.takipi.integrations.grafana.input.RegressionsNameInput;
import com.takipi.integrations.grafana.input.ViewInput;

public class RegressionNameFunction extends BaseNameFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new RegressionNameFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return RegressionsNameInput.class;
		}
		
		@Override
		public String getName() {
			return "regressionName";
		}
	}
	
	public RegressionNameFunction(ApiClient apiClient) {
		super(apiClient);
	}
	
	@Override
	protected String getName(ViewInput input, String serviceId) {
		
		RegressionsNameInput regNameInput = (RegressionsNameInput)input;
		
		RegressionInput regressionInput = new RegressionInput();
		
		regressionInput.serviceId = serviceId;
		regressionInput.viewId = getViewId(serviceId, input.view);
		
		if (regressionInput.viewId == null) {
			return null;
		}

		String result;
		Collection<String> deployments = input.getDeployments(serviceId);
		
		if ((deployments != null) && (deployments.size() > 0)) {
			regressionInput.activeTimespan = regNameInput.activeTimespan;
			regressionInput.baselineTimespan = regNameInput.baselineTimespan;

			regressionInput.applictations = input.getApplications(serviceId);
			regressionInput.servers = input.getServers(serviceId);
			regressionInput.deployments = deployments;
			
			result = RegressionStringUtil.getRegressionName(apiClient, regressionInput);
		} else {
			
			long duration = new DateTime().minusMinutes(regNameInput.baselineTimespan).getMillis();
			PrettyTime prettyTime = new PrettyTime();
	
			String activeWindowDuration = prettyTime.formatDuration(new Date(duration));
			result = "Comparing against the last " + activeWindowDuration;
		}

		return result;
	}
}
