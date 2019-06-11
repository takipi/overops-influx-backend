package com.takipi.integrations.grafana.input;

/**
 * A base for all input passed into functions including variable, annotation and widget functions.
 * All functions take the form of name({parameters}). The parameters are formatted as a json object
 * where each parameter is of the following format "name":"value". Value can either be supplied as a literall
 * or as a reference to a template variable which in itself can a constant, user selection, or reference to
 * another function call.
 * 
 * Here's an example for a template variable query that invokes the application function to populate
 * a list of available function to the user to filter data by:
 * 		applications({"environments":"$environments","sorted":"true"})
 * 
 * Its first parameters is a reference to the value of the template variable $environments which
 * lists the names of environments available to the user via their OO REST API key which was passed
 * to the Grafana data source to which this query is connected. The "sorted" parameter is a literal parameter 
 * value that controls whether the results returned by this function call are sorted.
 * 
 * The $environments variable itself is defined by a query invoking the environments function as follows:
 * 		environments({"sorted":"true"})
 * 
 *  A second type of template variable that can be populated by making a query to a function is that
 *  of an annotation used to add metadata to a Grafana dashboard, see here - http://docs.grafana.org/reference/annotations/
 *  This call is invoked as follows:
 *  		
 *  		deploymentsAnnotation({"graphType":"view","volumeType":"hits","view":"All Events",
 *  		"timeFilter":"time >= now() - 14d","environments":"$environments", 
 *  		"applications":"$applications", "servers":"$servers","deployments":"$deployments",
 *  		"seriesName":"Times","graphCount":3})
 *  
 * This call receives multiple parameters in order to add to the dashboard the date times in which
 * deployments were introduced into the environments. As such, it is controlled by multiple variables
 * which represent the choices made by the user to filter their dashboard data by envs, apps, servers, deployments.
 * This ensure that only deployments relevant to the user's selection are displayed.
 * 
 * The third, and most important, type of query is that used to populate the contents of a dashboard
 * widget. These follow the same syntax:
 * 
 * 		graph({"graphType":"view","volumeType":"all","view":"$view","timeFilter":"$timeFilter",
 * 		"environments":"$environments", "applications":"$applications", "servers":"$servers", 
 * 		"deployments":"$deployments","pointsWanted":"$pointsWanted","types":"$type",
 * 		seriesName":"Times", "transactions":"$transactions"})
 * 
 * This query populates a graph widget with the graph volumes of all "hits" volumes of events in
 * the selected env, app, servers, deployments, transactions and event types.
 * 
 * Another example for a query can that used to populate a list of events in a grid matching a
 * set of criteria.
 * 
 * events({"fields":"link,type,entry_point,introduced_by,message,error_location,stats.hits,
 * rate,first_seen","view":"$view","timeFilter":"$timeFilter","environments":"$environments",
 * "applications":"$applications","servers":"$servers","deployments":"$deployments",
 * "volumeType":"all","maxColumnLength":80, "types":"$type","pointsWanted":"$pointsWanted",
 * "transactions":"$transactions", "searchText":"$search"})
 * 
 * In this case the attributes selected in the "fields" variable of an event object
 *  (as defined in https://doc.overops.com/reference#get_services-env-id-events-event-id) are returned
 *  for all objects matching the selected env,app,server,deployment,transaction and event type.
 * 
 */
public abstract class FunctionInput {

	public enum TimeFormat {
		
		/**
		 * Time stamps are returned in UNIX Epoch numeric format. This is the default format
		 */
		EPOCH,
		
		/**
		 * Time stamps are returned in ISO format with UTZ Zone
		 */
		ISO_UTC
	}
	
	/**
	 * The time format in which any time stamps are returned when used in time series
	 */
	public TimeFormat timeFormat;
	
	public TimeFormat getTimeFormat() {
		
		if (timeFormat == null) {
			return TimeFormat.EPOCH;
		}
		
		return timeFormat;
	}
	
	/**
	 * The query sent by the user this function is processing 
	 */
	public String query;
	
}
