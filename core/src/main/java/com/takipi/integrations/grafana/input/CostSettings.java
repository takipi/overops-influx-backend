package com.takipi.integrations.grafana.input;

import java.util.HashMap;
import com.takipi.integrations.grafana.functions.GrafanaFunction;

public class CostSettings {

	public Double costHigherThan;
	
	public HashMap<String,Double> costMatrix = new HashMap<>();
	
	public Double calculateCost(String eventType) {
		Double result = .0;
		
		if (eventType != null) {
			String eventTypeIn = eventType.trim(); 
			if (!eventTypeIn.isEmpty()) {
				if (costMatrix.containsKey(eventTypeIn)) {
					result = costMatrix.get(eventTypeIn);
				} else if (costMatrix.containsKey("")) {
					result = costMatrix.get("");
				} 
			}
		}
				
		return result;
	}

	public Double calculateAbbrCost(String eventTypeAbbr) {
		return calculateCost(GrafanaFunction.getTypesMapSpecular().get(eventTypeAbbr));
	}

}
