package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Objects;
import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.util.ArrayUtil;

/**
 The base input for all template variable functions that operate on specific 
 * set of selected environments. 
 * 
*/

public abstract class BaseEnvironmentsInput extends VariableInput {

	/**
	 * A comma delimited array of environments names to use as a filter. 
	 * Specify null, "", "all" or "*" to clear selection, in which case no environments are selected.
	 */
	public String environments;

	public static int MAX_COMBINE_SERVICES = 3;
	
	/**
	 * Control whether the information returned is limited to MAX_COMBINE_SERVICES,
	 * or allows the user to selected an unlimited number of envs to query.
	 */
	public boolean unlimited;
	
	public static List<String> getServiceIds(String environments) {
		if (!hasFilter(environments)) {
			return Collections.emptyList();
		}

		String[] serviceIds = ArrayUtil.safeSplitArray(environments, GrafanaFunction.GRAFANA_SEPERATOR, false);
	
		if ((serviceIds.length == 1) && (serviceIds[0].equals("()"))) {
			return Collections.emptyList();
		}
		
		if ((serviceIds.length == 1) && (environments.contains(GrafanaFunction.ARRAY_SEPERATOR_RAW))) {
			serviceIds = ArrayUtil.safeSplitArray(environments, GrafanaFunction.ARRAY_SEPERATOR, false);
		}
		
		List<String> result = new ArrayList<String>();

		for (int i = 0; i < serviceIds.length; i++) {
			
			String service = serviceIds[i];
			String value = service.replace("(", "").replace(")", "");
			
			String[] parts = value. split(GrafanaFunction.SERVICE_SEPERATOR);

			String serviceId;
			
			if (parts.length > 1) {
				serviceId = parts[parts.length - 1];
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
	
	public List<String> getServiceIds() {
		return getServiceIds(environments);
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if (!(obj instanceof BaseEnvironmentsInput)) {
			return false;
		}
		
		return Objects.equal(this.environments, ((BaseEnvironmentsInput)obj).environments);
	}
	
	@Override
	public int hashCode() {
		
		if (environments == null) {
			return super.hashCode();
		}
		
		return environments.hashCode();
	}
}
