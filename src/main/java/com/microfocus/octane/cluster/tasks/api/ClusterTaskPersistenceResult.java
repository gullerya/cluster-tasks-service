package com.microfocus.octane.cluster.tasks.api;

/**
 * Created by gullery on 16/12/2016.
 *
 * DTO describing the result of single task persistence attempt into the data storage
 */

public final class ClusterTaskPersistenceResult {
	public final Long id;
	public final CTPPersistStatus status;

	public ClusterTaskPersistenceResult(Long id) {
		this.id = id;
		this.status = CTPPersistStatus.SUCCESS;
	}

	public ClusterTaskPersistenceResult(CTPPersistStatus status) {
		this.id = null;
		this.status = status;
	}
}
