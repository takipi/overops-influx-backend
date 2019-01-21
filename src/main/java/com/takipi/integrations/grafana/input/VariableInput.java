package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.util.ArrayUtil;

/**
 * The base function input for all functions that are used to populate template variables:
 * http://docs.grafana.org/reference/templating/ 
 */
public abstract class VariableInput extends FunctionInput {
	
	 /** Control whether values of this variable are sorted alphabetically or logically */
	public boolean sorted;
	
	 /** Controls whether values are added as separate items, or as one comma delimited value */
	public boolean commaDelimited;
	
	public static boolean hasFilter(String value) {
		return (value != null) && (value.length() != 0) && (!GrafanaFunction.VAR_ALL.contains(value));
	}
	
	public static List<String> getServiceFilters(String value, String serviceId, boolean matchCase) {

		if (!hasFilter(value)) {
			return Collections.emptyList();
		}

		String[] values = ArrayUtil.safeSplitArray(value, GrafanaFunction.GRAFANA_SEPERATOR, false);
		Set<String> result = new HashSet<String>();

		for (int i = 0; i < values.length; i++) {
			String service = values[i];
			String clean = service.replace("(", "").replace(")", "");
			
			if (clean.length() == 0) {
				continue;
			}

			if (!matchCase) {
				clean = clean.toLowerCase();
			}

			String[] parts = clean.split(GrafanaFunction.SERVICE_SEPERATOR);

			if ((serviceId != null) && (parts.length > 1)) {
				if (!serviceId.equals(parts[1])) {
					continue;
				}

				result.add(parts[0]);
			} else {
				result.add(clean);
			}
		}

		return new ArrayList<String>(result);
	}
}
