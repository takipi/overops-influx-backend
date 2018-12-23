package com.takipi.integrations.grafana.input;


/**
 * This function provides a way to pass a link to the OverOps backend server connected to 
 * the query's data source (on-premises) or SaaS. This can be used to link a widget to an OverOps ARC link
 * or dashboard view.
 * 
 * The function can be used to retrieve either the backend URL, port or combination of both
 * depending on the value of the "type" value which can be set to either: 
 * 		URL: the complete URL to the OverOps backend including port (i.e. app.overops.com:443)
 * 		PORT: the port number (i.e. app.overops.com)
 * 		URL_NO_PORT: the URL without a port value  (i.e 443) 
 *
 * Example query: 
 * 		apiHost({"type":"PORT"})
 */
public class ApiHostInput extends VariableInput
{
	public enum HostType {
		URL, PORT, URL_NO_PORT;
	}
	
	public HostType type;
	
}
