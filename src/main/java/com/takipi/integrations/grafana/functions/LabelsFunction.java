package com.takipi.integrations.grafana.functions;

import java.util.regex.Pattern;

import com.takipi.common.api.ApiClient;
import com.takipi.common.api.data.label.Label;
import com.takipi.common.api.request.label.LabelsRequest;
import com.takipi.common.api.result.label.LabelsResult;
import com.takipi.common.api.url.UrlClient.Response;
import com.takipi.integrations.grafana.input.EnvironmentsInput;
import com.takipi.integrations.grafana.input.LabelsInput;

public class LabelsFunction extends EnvironmentVariableFunction {
	
	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new LabelsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return LabelsInput.class;
		}
		
		@Override
		public String getName() {
			return "labels";
		}
	}
	
	public LabelsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, String[] serviceIds, String serviceId,
			VariableAppender appender) {
		
		if (!(input instanceof LabelsInput)) {
			throw new IllegalArgumentException("input");
		}
		
		LabelsInput labelsInput = (LabelsInput)input;
		
		LabelsRequest request = LabelsRequest.newBuilder().setServiceId(serviceId).build();
		Response<LabelsResult> response = apiClient.get(request);
		
		validateResponse(response);
		
		if ((response.data == null) || (response.data.labels == null)) {
			return;
		}
		
		Pattern filterPattern;
		
		if (labelsInput.lablesRegex != null) {
			filterPattern = Pattern.compile(labelsInput.lablesRegex);
		} else {
			filterPattern = null;
		}
		
		for (Label label : response.data.labels) {
			
			if ((filterPattern != null) && (!filterPattern.matcher(label.name).find())) {
				continue;
			}
			
			String labelName = getServiceValue(label.name, serviceId, serviceIds);
			appender.append(labelName);
		}
	}
}
