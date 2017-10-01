package com.microfocus.octane.cluster.tasks.api;

/**
 * Created by gullery on 16/12/2016.
 *
 * DTO describing the result of single task persistence attempt into the data storage
 */

public final class ClusterTaskPersistenceResult {
	public final CTPPersistStatus status;

	public ClusterTaskPersistenceResult(CTPPersistStatus status) {
		this.status = status;
	}
}
