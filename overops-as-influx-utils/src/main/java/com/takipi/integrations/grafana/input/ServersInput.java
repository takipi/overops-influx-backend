package com.takipi.integrations.grafana.input;

/**
 * 
 * This function returns the list of active servers within the target environments.
 * 
 * 	Example query:
 * 		servers({"environments":"$environments","sorted":"true"})
 * 
 * 	Screenshot: https://drive.google.com/file/d/1nR2HMzxTyL3NhA6Ss70pFnVO1SAYQ15A/view?usp=sharing
 */
public class ServersInput extends BaseEnvironmentsInput {
	
}
