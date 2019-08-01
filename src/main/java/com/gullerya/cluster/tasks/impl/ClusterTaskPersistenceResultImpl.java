package com.gullerya.cluster.tasks.impl;

import com.gullerya.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.gullerya.cluster.tasks.api.enums.ClusterTaskInsertStatus;

/**
 * Created by gullery on 16/12/2016.
 */

class ClusterTaskPersistenceResultImpl implements ClusterTaskPersistenceResult {
	private final ClusterTaskInsertStatus status;

	ClusterTaskPersistenceResultImpl(ClusterTaskInsertStatus status) {
		this.status = status;
	}

	@Override
	public ClusterTaskInsertStatus getStatus() {
		return status;
	}
}
