package com.takipi.integrations.grafana.input;

import java.util.Collection;
import java.util.Collections;

import com.google.common.base.Objects;

/**
 * The base input for all functions that operate on events nested within a target view,
 * whose entry points match a list of selected entry points  within a selected time range.
 *
 */
public class ViewInput extends EnvironmentsFilterInput {
	
	/**
	 * The name of the view to query for events. For example: "All Events".
	 */
	public String view;
	
	/**
	 * A comma delimited list of entry points in either simple class name or simple class name + method format.
	 * For example: "myServlet,"myOtherServlet.doGet" will only choose events whose entry point
	 * is "myServlet" (regardless of a method name) or whose entry point class is "myOtherServlet" and
	 * method name is "doGet".
	 */
	public String transactions;
	
	/**
	 * A time filter denoting the time range in which this query operates. The format os the time filter
	 * should match the Grafana time range format: http://docs.grafana.org/reference/timerange/
	 */
	public String timeFilter;
	
	
	public static final String FROM = "from";
	public static final String TO = "to";
	public static final String TIME_RANGE = "timeRange";
	
	public boolean hasTransactions() {
		return hasFilter(transactions);
	}
	
	public Collection<String> getTransactions(String serviceId) {

		if (!hasTransactions()) {
			return Collections.emptyList();
		}

		Collection<String> result = getServiceFilters(transactions, serviceId, true);
		
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if (!super.equals(obj)) {
			return false;
		}
		
		if (!(obj instanceof ViewInput)) {
			return false;
		}
		
		ViewInput other = (ViewInput)obj;
		
		return Objects.equal(view, other.view) 
				&& Objects.equal(transactions, other.transactions)
				&& Objects.equal(timeFilter, other.timeFilter);
	}
	
	@Override
	public int hashCode() {
		
		if (view != null) {
			return super.hashCode() ^ view.hashCode();
		}
		
		return super.hashCode();
	}
}

