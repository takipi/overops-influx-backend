package com.takipi.integrations.grafana.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.api.client.data.deployment.SummarizedDeployment;
import com.takipi.api.client.result.deployment.DeploymentsResult;
import com.takipi.api.core.url.UrlClient.Response;

public class DeploymentUtil {
	
	public static void sortDeployments(List<String> deployments) {
		
		deployments.sort(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return compareDeployments(o1, o2);
			}
		});
		
	}
	
	public static Collection<SummarizedDeployment> getDeployments(ApiClient apiClient, String serviceId, boolean active) {
		
		Response<DeploymentsResult> response = ApiCache.getDeployments(apiClient, serviceId, active);
		
		if ((response == null) || (response.data == null)) {
			return Collections.emptyList();
		}
		
		return response.data.deployments;
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
