package com.takipi.integrations.grafana.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.result.metrics.system.SystemMetricMetadataResult;
import com.takipi.api.client.result.metrics.system.SystemMetricMetadatasResult;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.integrations.grafana.input.SystemMetricsMetadataInput;
import com.takipi.integrations.grafana.util.ApiCache;

public class SystemMetricsMetadata {
	
	public String serviceId;
	
	public Map<String, SystemMetric> metricMap;
	public List<SystemMetric> metricList;
	
	protected SystemMetricsMetadata(String serviceId) {
		this.serviceId = serviceId;
		this.metricMap = new HashMap<String, SystemMetric>();
		this.metricList = new ArrayList<SystemMetric>();
	}
	
	public static class SystemMetric {
		
		public String name;
		public SystemMetricMetadataResult[] metadatas;
		
		public SystemMetric(String name, SystemMetricMetadataResult metadata) {
			this.name = name;
			this.metadatas = new SystemMetricMetadataResult[] {metadata};
		}
		
		public SystemMetric(String name, SystemMetricMetadataResult first,
			SystemMetricMetadataResult second) {
			this.name = name;
			this.metadatas = new SystemMetricMetadataResult[] {first ,second};		}
		
		/**
		 * @param values - for children 
		 */
		public double getValue(double[] values) {
			return values[0];
		}
		
		protected boolean hasUnit() {
			return metadatas.length == 1;
		}
		
		public String getUnit() {
			
			if (hasUnit()) {
				
				if (metadatas[0].unit != null) {
					return metadatas[0].unit.type;
				}
			}
			
			return null;
		}
	}
	
	protected static class CpuLoadSystemMetric extends SystemMetric {

		public CpuLoadSystemMetric(SystemMetricMetadataResult first, SystemMetricMetadataResult second){
			super("CPU Usage", first, second);
		}
		
		@Override
		public double getValue(double[] values) {
			
			double cpuLoadAvg5 = values[0];
			double availProcessors = values[1];
	
			if (availProcessors == 0) {
				return 0;
			}
				
			return cpuLoadAvg5 / availProcessors;
		}
	}
	
	protected static class MemSystemMetric extends SystemMetric {

		public MemSystemMetric(SystemMetricMetadataResult first, SystemMetricMetadataResult second){
			super("Memory % Used", first, second);
		}
		
		@Override
		public double getValue(double[] values) {
			
			double heapUsed = values[0];
			double heapMax = values[1];
	
			if (heapMax == 0) {
				return 0;
			}
				
			return heapUsed / heapMax;
		}	
	}
	
	protected static class GCObjectsSystemMetric extends SystemMetric {

		public GCObjectsSystemMetric(SystemMetricMetadataResult first, SystemMetricMetadataResult second){
			super("GC Objects", first, second);
		}
		
		@Override
		protected boolean hasUnit()  {
			return true;
		}
		
		@Override
		public double getValue(double[] values) {
			
			double scanagedGC = values[0];
			double markSweepGC = values[1];
	
			return scanagedGC + markSweepGC;
		}
	}
	
	protected static class GCTimeSystemMetric extends SystemMetric {

		public GCTimeSystemMetric(SystemMetricMetadataResult first, SystemMetricMetadataResult second){
			super("GC Time", first, second);
		}
		
		@Override
		protected boolean hasUnit()  {
			return true;
		}
		
		@Override
		public double getValue(double[] values) {
			
			double scanagedTime = values[0];
			double markSweepTime = values[1];
	
			return scanagedTime + markSweepTime;
		}
	}
	
	private SystemMetric find(String name) {
		
		for (SystemMetric sm : metricList) {
			if (sm.metadatas[0].name.equals(name)) {
				return sm;
			}
		}
		
		return null;
	}

	private SystemMetricMetadataResult findMetadata(String name) {
		
		SystemMetric systemMetric = find(name);
		
		if (systemMetric != null) {
			return systemMetric.metadatas[0];

		}
		
		return null;
	}
	
	private void init(ApiClient apiClient) {
		
		Response<SystemMetricMetadatasResult> response = 
				ApiCache.getSystemMetricMetadatas(apiClient, serviceId);
		
		if ((response.isBadResponse()) || (response.data == null)
		||  (response.data.metrics == null)) {
			return;
		}
		
		for (SystemMetricMetadataResult metadata : response.data.metrics) {
			SystemMetric systemMetric = new SystemMetric(metadata.display_name, metadata);
			metricList.add(systemMetric);
		}
		
		SystemMetricMetadataResult cpuLoad5 = findMetadata("cpu-load-avg-5");
		SystemMetricMetadataResult procAvail = findMetadata("cpu-available-processors");

		List<SystemMetric> syntethicMetrics = new ArrayList<SystemMetric>();
		
		if ((cpuLoad5 != null) && procAvail != null) {
			syntethicMetrics.add(new CpuLoadSystemMetric(cpuLoad5, procAvail));
		}
		
		SystemMetricMetadataResult heapUsed = findMetadata("memory-heap-used");
		SystemMetricMetadataResult heapMax = findMetadata("memory-heap-max");

		if ((heapUsed != null) && heapMax != null) {
			syntethicMetrics.add(new MemSystemMetric(heapUsed, heapMax));		
		}
			
		SystemMetricMetadataResult gcScavange = findMetadata("gc-scavenge-collections-count");
		SystemMetricMetadataResult gcMarkSweep = findMetadata("gc-marksweep-collections-count");

		if ((gcScavange != null) && gcMarkSweep != null) {
			syntethicMetrics.add(new GCObjectsSystemMetric(gcScavange, gcMarkSweep));
		}
		
		SystemMetricMetadataResult gcScvngTime = findMetadata("gc-scavenge-collections-time");
		SystemMetricMetadataResult gcSweepTime = findMetadata("gc-marksweep-collections-time");

		if ((gcScvngTime != null) && gcSweepTime != null) {
			syntethicMetrics.add(new GCTimeSystemMetric(gcScvngTime, gcSweepTime));
		}
				
		List<SystemMetric> prioSystemMetrics = new ArrayList<SystemMetric>();
		
		for (String prioMetricName : SystemMetricsMetadataInput.PRIO_SYSTEM_METRICS) {
			
			SystemMetric prioSystemMetric = find(prioMetricName);
			
			if (prioSystemMetric != null) {
				prioSystemMetrics.add(prioSystemMetric);
				metricList.remove(prioSystemMetric);	
			}
		} 
		
		metricList.addAll(0, prioSystemMetrics);
		metricList.addAll(0, syntethicMetrics);

		for (SystemMetric systemMetric : metricList) {
			metricMap.put(systemMetric.name.toLowerCase(), systemMetric);
		}
	}
	
	public static SystemMetricsMetadata of(ApiClient apiClient, String serviceId) {
		SystemMetricsMetadata result = new SystemMetricsMetadata(serviceId);
		result.init(apiClient);
		return result;
	}
}
