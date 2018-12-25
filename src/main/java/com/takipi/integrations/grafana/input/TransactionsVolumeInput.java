package com.takipi.integrations.grafana.input;

/**
 * A function returning single stat volume of transactions (i.e. calls into event entry points)
 * with a target set of envs, apps, deployments and servers. 
 *
 *	Example query:
 *
 *		transactionsVolume({"type":"sum","volumeType":"invocations","view":"$view",
 *		"timeFilter":"$timeFilter","environments":"$environments", "applications":"$applications",
 *		"servers":"$servers", "transactions":"$transactions", "searchText":"$searchText",
 *		"pointsWanted":"$transactionPointsWanted"})
 *
 *		Screenshot: 
 *			https://drive.google.com/file/d/11z_BD_B1Zno7uaWYGX4pTprJzqIwmtsJ/view?usp=sharing
 *			https://drive.google.com/file/d/1i_9DjK-mugjsagBKh-ZuJb3G07AtcyH6/view?usp=sharing
 */
public class TransactionsVolumeInput extends BaseVolumeInput {
	
	/**
	 * The type of volume to return
	 */
	public enum TransactionVolumeType {
		
		/**
		 * The number if calls into event entry points
		 */
		invocations, 
		
		
		/**
		 * The avg time of calls to entry points to complete
		 */
		avg, 
		
		/**
		 * The unique number of entry points
		 */
		count;
	}
	
	/**
	 * The volume type to return by querying this function
	 */
	public TransactionVolumeType volumeType;		
}
