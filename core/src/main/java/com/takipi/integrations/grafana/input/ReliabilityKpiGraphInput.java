package com.takipi.integrations.grafana.input;

public class ReliabilityKpiGraphInput extends GraphInput {
	
	public enum ReliabilityKpi {
		
		NewErrors,
		SevereNewErrors,
		IncreasingErrors,
		SevereIncreasingErrors,
		Slowdowns,
		SevereSlowdowns,
		ErrorVolume,
		ErrorCount,
		ErrorRate,
		Score
	}
	
	public String kpi;
	
	public ReliabilityKpi getKpi() {
		
		if ((kpi ==  null) || (kpi.length() == 0)) {
			return ReliabilityKpi.ErrorVolume;
		}
		
		ReliabilityKpi result = ReliabilityKpi.valueOf(kpi.replace(" ", ""));
		
		return result;
	}
	
	public int limit;
	
	public String reportInterval;
	
	public int transactionPointsWanted;
	
	public boolean aggregate;
	
}
