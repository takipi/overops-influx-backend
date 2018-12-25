package com.takipi.integrations.grafana.input;


/**
 * This function provides a way to link a widget to the OverOps backend server represented by  
 * the query's data source, either on-premises or SaaS. This can be used to link a widget to an OverOps ARC link
 * or dashboard view.
 * 
 * The function can be used to retrieve either the backend URL, port or combination of both
 * depending on the value of the "type" value,
 *
 * Example query: 
 * 		apiHost({"type":"PORT"})
 */

public class ApiHostInput extends VariableInput
{
	/**
	 * The function can be used to retrieve either the backend URL, port or combination of both,
	 * depending on the value of the "type" value which can be set to either: 
	 * 		URL: the complete URL to the OverOps backend including port (i.e. app.overops.com:443)
	 * 		PORT: the port number (i.e. app.overops.com)
	 * 		URL_NO_PORT: the URL without a port value  (i.e 443) 
	 */
	public enum HostType {
		URL, 
		PORT, 
		URL_NO_PORT;
	}
	
	/**
	 *  The URL type to return as one of the HostType options
	 */
	public HostType type;
	
}
