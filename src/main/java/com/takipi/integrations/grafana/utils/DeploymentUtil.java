package com.takipi.integrations.grafana.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Lists;
import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.deployment.SummarizedDeployment;
import com.takipi.api.client.request.deployment.DeploymentsRequest;
import com.takipi.api.client.result.deployment.DeploymentsResult;
import com.takipi.api.client.util.client.ClientUtil;
import com.takipi.api.core.url.UrlClient.Response;
import com.takipi.common.util.Pair;
import com.takipi.integrations.grafana.input.EnvironmentsFilterInput;

public class DeploymentUtil {
	
	private static List<String> getActiveDeployments(ApiClient apiClient, String serviceId) {
		
		DeploymentsRequest request = DeploymentsRequest.newBuilder().setServiceId(serviceId).setActive(true).build();

		Response<DeploymentsResult> response = apiClient.get(request);

		if ((response.isBadResponse()) || (response.data == null)) {
			throw new IllegalStateException(
					"Could not acquire deployments for service " + serviceId + " . Error " + response.responseCode);
		}

		if (response.data.deployments == null) {
			return Collections.emptyList();
		}

		List<String> result = Lists.newArrayListWithCapacity(response.data.deployments.size());

		for (SummarizedDeployment deployment : response.data.deployments) {
			result.add(deployment.name);
		}

		return result;
	}
	
	private static String getStartDeployment(ApiClient apiClient, EnvironmentsFilterInput input, String serviceId) {
		
		List<String> inputDeployments = input.getDeployments(serviceId);
		
		String result;
		
		if (inputDeployments.size() == 1) {
			result = inputDeployments.get(0);
		} else {
			
			List<String> activeDeployments = getActiveDeployments(apiClient, serviceId);
			
			if (activeDeployments.size() == 0) {
				return null;
			}
			
			result = activeDeployments.get(0);
		}
		
		return result;
	}
	
	public static void sortDeployments(List<String> deployments) {
		
		deployments.sort(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return compareDeployments(o1, o2);
			}
		});
		
	}
	
	private static List<String> getAllDeployments(ApiClient apiClient, String serviceId) {
		
		List<String> result = ClientUtil.getDeployments(apiClient, serviceId);
		sortDeployments(result);
		
		return result;
	}
	
	public static Pair<String, List<String>> getActiveDeployment(ApiClient apiClient,
			EnvironmentsFilterInput input, String serviceId, int depCount) 
	{		
		String deployment = getStartDeployment(apiClient, input, serviceId);
			
		List<String> allDeployments = getAllDeployments(apiClient, serviceId);	
		
		if (allDeployments == null) {
			return null;
		}
		
		if (deployment == null) {
			return null;
		}
	
		if (depCount == 0) {
			return null;
		}
		
		List<String> prevDeps = new ArrayList<String>();
		
		int index = allDeployments.indexOf(deployment);
		
		if ((index != -1) && (index < allDeployments.size() - 1)) {
			
			int seriesCount;
			
			if (index + depCount >= allDeployments.size()) {
				seriesCount = allDeployments.size() - index - 1;
			} else {
				seriesCount = depCount;
			}
						
			for (int i = 0; i < seriesCount; i++) {
				
				String prevDeployment = allDeployments.get(index + 1 + i);
				prevDeps.add(prevDeployment);	
			}
		}
		
		return Pair.of(deployment, prevDeps);
	}
	
	public static int compareDeployments(Object o1, Object o2) {
		
		double i1 = getDeplyomentNumber(o1.toString());
		double i2 = getDeplyomentNumber(o2.toString());

		double d = i2 - i1;

		if (d == 0) {
			return 0;
		}

		if (d < 0) {
			return -1;
		}

		return 1;

	}
	
	private static double getDeplyomentNumber(String value) {

		boolean hasDot = false;
		boolean hasNums = false;

		StringBuilder number = new StringBuilder();

		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);

			if (c == '.') {
				if (!hasDot) {
					number.append(c);
					hasDot = true;
				}
				continue;
			}

			if ((c >= '0') && (c <= '9')) {
				number.append(c);
				hasNums = true;
			}
		}

		if (hasNums) {
			double result = Double.parseDouble(number.toString());
			return result;
		} else {
			return -1;
		}
	}
}
