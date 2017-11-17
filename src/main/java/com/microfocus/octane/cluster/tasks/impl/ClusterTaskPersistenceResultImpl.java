package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.microfocus.octane.cluster.tasks.api.enums.CTPPersistStatus;

/**
 * Created by gullery on 16/12/2016.
 */

class ClusterTaskPersistenceResultImpl implements ClusterTaskPersistenceResult {
	private final CTPPersistStatus status;

	ClusterTaskPersistenceResultImpl(CTPPersistStatus status) {
		this.status = status;
	}

	@Override
	public CTPPersistStatus getStatus() {
		return status;
	}
}
