package com.takipi.integrations.grafana.input;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.util.ArrayUtil;

public class EventTypesInput extends ViewInput {
	
	public String types; 

	public Collection<String> getTypes() {
		
		if (types == null) {
			return Collections.emptyList();
		}

		return Arrays.asList(ArrayUtil.safeSplitArray(types, GrafanaFunction.GRAFANA_SEPERATOR, false));

	}
}

