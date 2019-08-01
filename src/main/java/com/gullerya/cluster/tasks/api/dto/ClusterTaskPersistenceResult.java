package com.gullerya.cluster.tasks.api.dto;

import com.gullerya.cluster.tasks.api.enums.ClusterTaskInsertStatus;

/**
 * Created by gullery on 16/12/2016.
 *
 * DTO describing the result of single task persistence attempt into the data storage
 */

public interface ClusterTaskPersistenceResult {
	ClusterTaskInsertStatus getStatus();
}
