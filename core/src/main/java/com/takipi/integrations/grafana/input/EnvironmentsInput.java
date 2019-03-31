package com.takipi.integrations.grafana.input;

/**
* 2. As the input for the environments functions that is used to populate a template variable 
* containing the names of all environments to which the OO API key provided to the Grafana datasource 
* connected to this query has access to. 
* 
* Example query:
* 		environments({"sorted":"true"})
* 
* Screenshot: https://drive.google.com/file/d/187V1IuD5PeC9cz9sd4nzpY12q2PxpmW9/view?usp=sharing
*
*/
public class EnvironmentsInput extends BaseEnvironmentsInput {
}
