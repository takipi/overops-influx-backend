package com.takipi.integrations.grafana.input;

import com.takipi.api.client.util.validation.ValidationUtil.VolumeType;

/**
 * This function returns a single stat value depicting the avg/sum/unique count of a target event
 * set's volume.
 *
 *	Example query:
 *		volume({"type":"count","volumeType":"hits","eventVolumeType":"hits","view":"$view",
 *		"timeFilter":"$timeFilter","environments":"$environments", "applications":"$applications",
 *		"servers":"$servers","deployments":"$deployments","types":"$type",
 *		"transactions":"$transactions","filter":"events", 
 *		"pointsWanted":"$pointsWanted"})
 *
 *		Screenshot: https://drive.google.com/file/d/1GXyStXf4yFRn4mKRRpJLNMcdXknwdM80/view?usp=sharing
 */

public class VolumeInput extends BaseVolumeInput {
	
	/**
	 * The volume type aggregated by this function.
	 * 		hits, all: aggregate the event volume.
	 * 		invocations: aggregate the ratio between even volume and the calls into events locations.
	 */
	public VolumeType volumeType;
}
