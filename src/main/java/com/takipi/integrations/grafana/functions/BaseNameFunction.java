
package com.takipi.integrations.grafana.functions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.takipi.api.client.ApiClient;
import com.takipi.integrations.grafana.input.FunctionInput;
import com.takipi.integrations.grafana.input.ViewInput;
import com.takipi.integrations.grafana.output.Series;

public abstract class BaseNameFunction extends GrafanaFunction {

	public BaseNameFunction(ApiClient apiClient) {
		super(apiClient);
	}

	protected abstract String getName(ViewInput input, String serviceId);

	@Override
	public List<Series> process(FunctionInput functionInput) {

		if (!(functionInput instanceof ViewInput)) {
			throw new IllegalArgumentException("functionInput");
		}

		ViewInput input = (ViewInput)functionInput;
		
		String[] serviceIds = getServiceIds(input);

		if (serviceIds.length == 0) {
			return null;
		}

		String name = getName(input, serviceIds[0]);

		if (name == null) {
			return null;
		}

		Series series = new Series();

		series.name = SERIES_NAME;
		series.columns = Arrays.asList(new String[] { KEY_COLUMN, VALUE_COLUMN });
		series.values = Collections.singletonList(Arrays.asList(new Object[] { KEY_COLUMN, name }));

		return Collections.singletonList(series);
	}

}
