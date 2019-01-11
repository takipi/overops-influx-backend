package com.takipi.integrations.grafana.input;

import java.util.HashMap;

public class CostData {

	public Double costHigherThan;
	
	public HashMap<String,Double> costMatrix = new HashMap<>();
	
	public double calculateCost(String eventType) {
		double result = .0;
		
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
}
