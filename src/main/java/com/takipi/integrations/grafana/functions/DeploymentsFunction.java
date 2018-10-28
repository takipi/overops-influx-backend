package com.takipi.integrations.grafana.functions;

import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.integrations.grafana.input.EnvironmentsInput;

public class DeploymentsFunction extends EnvironmentVariableFunction {

	public static class Factory implements FunctionFactory {

		@Override
		public GrafanaFunction create(ApiClient apiClient) {
			return new DeploymentsFunction(apiClient);
		}

		@Override
		public Class<?> getInputClass() {
			return EnvironmentsInput.class;
		}

		@Override
		public String getName() {
			return "deployments";
		}
	}

	public DeploymentsFunction(ApiClient apiClient) {
		super(apiClient);
	}

	@Override
	protected void populateServiceValues(EnvironmentsInput input, String[] serviceIds, String serviceId,
			VariableAppender appender) {

		List<String> serviceDeps = ClientUtil.getDeployments(apiClient, serviceId);

		for (String dep : serviceDeps) {

			String depName = getServiceValue(dep, serviceId, serviceIds);
			appender.append(depName);
		}
	}

	@Override
	protected int compareValues(Object o1, Object o2) {
		return compare(o1, o2);
	}

	public static int compare(Object o1, Object o2) {
		double i1 = getDeplyomentNumber(o1.toString());
		double i2 = getDeplyomentNumber(o2.toString());

		double d = i2 - i1;

		if (d == 0) {
			return 0;
		}

		if (d < 0) {
			return -1;
		}

		return 1;

	}
	
	public static double getDeplyomentNumber(String value) {

		boolean hasDot = false;
		boolean hasNums = false;

		StringBuilder number = new StringBuilder();

		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);

			if (c == '.') {
				if (!hasDot) {
					number.append(c);
					hasDot = true;
				}
				continue;
			}

			if ((c >= '0') && (c <= '9')) {
				number.append(c);
				hasNums = true;
			}
		}

		if (hasNums) {
			double result = Double.parseDouble(number.toString());
			return result;
		} else {
			return -1;
		}
	}
}
