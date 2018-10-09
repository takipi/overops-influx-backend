package com.takipi.integrations.grafana.input;

import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.utils.ArrayUtils;

public class EnvironmentsInput extends FunctionInput {
	
	public String environments;

	public String[] getServiceIds() {

		if (GrafanaFunction.VAR_ALL.equals(environments)) {
			return new String[0];
		}

		String[] services = ArrayUtils.safeSplitArray(environments, GrafanaFunction.GRAFANA_SEPERATOR, false);
		String[] result = new String[services.length];

		for (int i = 0; i < services.length; i++) {
			String service = services[i];
			String value = service.replace("(", "").replace(")", "");
			String[] parts = value.split(GrafanaFunction.SERVICE_SEPERATOR);

			if (parts.length > 1) {
				result[i] = parts[1];
			} else {
				result[i] = value;
			}
		}

		return result;
	}
}