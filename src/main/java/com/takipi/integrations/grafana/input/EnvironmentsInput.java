package com.takipi.integrations.grafana.input;

import com.google.common.base.Objects;
import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.utils.ArrayUtils;

public class EnvironmentsInput extends VariableInput {
	
	public String environments;

	public String[] getServiceIds() {

		if (GrafanaFunction.VAR_ALL.contains(environments)) {
			return new String[0];
		}

		String[] serviceIds = ArrayUtils.safeSplitArray(environments, GrafanaFunction.GRAFANA_SEPERATOR, false);
		String[] result = new String[serviceIds.length];

		for (int i = 0; i < serviceIds.length; i++) {
			
			String service = serviceIds[i];
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
	
	@Override
	public boolean equals(Object obj) {
		
		if (!(obj instanceof EnvironmentsInput)) {
			return false;
		}
		
		return Objects.equal(this.environments, ((EnvironmentsInput)obj).environments);
	}
	
	@Override
	public int hashCode() {
		
		if (environments == null) {
			return super.hashCode();
		}
		
		return environments.hashCode();
	}
}
