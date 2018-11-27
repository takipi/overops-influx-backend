package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Objects;
import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.util.ArrayUtil;

public class EnvironmentsInput extends VariableInput {
	
	public String environments;

	public List<String> getServiceIds() {

		if (!EnvironmentsFilterInput.hasFilter(environments)) {
			return Collections.emptyList();
		}

		String[] serviceIds = ArrayUtil.safeSplitArray(environments, GrafanaFunction.GRAFANA_SEPERATOR, false);
	
		if ((serviceIds.length == 1) && (serviceIds[0].equals("()"))) {
			return Collections.emptyList();
		}
		
		List<String> result = new ArrayList<String>();

		for (int i = 0; i < serviceIds.length; i++) {
			
			String service = serviceIds[i];
			String value = service.replace("(", "").replace(")", "");
			String[] parts = value.split(GrafanaFunction.SERVICE_SEPERATOR);

			String serviceId;
			
			if (parts.length > 1) {
				serviceId = parts[1];
			} else {
				serviceId = value;
			}
			
			if (service.startsWith(GrafanaFunction.GRAFANA_VAR_PREFIX)) {
				continue;
			}
			
			result.add(serviceId);
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
