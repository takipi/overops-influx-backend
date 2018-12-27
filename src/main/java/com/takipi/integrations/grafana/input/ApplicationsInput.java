package com.takipi.integrations.grafana.input;

/**
 * 
 * This function returns the list of active applications within the target environments. If
 * application groups are defined in the Settings dashboard, they are added before the 
 * individual application names. 
 * 
 * 	Example query:
 * 		applications({"environments":"$environments","sorted":"true"})
 * 
 * 	Screenshot: https://drive.google.com/file/d/1FblzDqI5NeMA9Zt1rNdMDE36132kMCDR/view?usp=sharing
 */

public class ApplicationsInput extends BaseEnvironmentsInput {
	
}
