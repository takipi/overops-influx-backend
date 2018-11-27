package com.takipi.integrations.grafana.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.takipi.integrations.grafana.functions.GrafanaFunction;
import com.takipi.integrations.grafana.functions.RegressionFunction.RegressionType;

public class RegressionsInput extends EventsInput
{
	
	public enum RenderMode
	{
		SingleStat,
		Grid,
		Graph;
	}
	
	public RenderMode render;
	public String regressionTypes;
	public String singleStatFormat;
	
	public Collection<RegressionType> getRegressionTypes()
	{
		
		if (regressionTypes == null)
		{
			return Collections.emptyList();
		}
		
		String[] parts = regressionTypes.split(GrafanaFunction.ARRAY_SEPERATOR);
		Collection<RegressionType> result = new ArrayList<RegressionType>(parts.length);
		
		for (String part : parts)
		{
			RegressionType type = RegressionType.valueOf(part);
			
			if (type != null)
			{
				result.add(type);
			}
		}
		
		return result;
		
	}
}
