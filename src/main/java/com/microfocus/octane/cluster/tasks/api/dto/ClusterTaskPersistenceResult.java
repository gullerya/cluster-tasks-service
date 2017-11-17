package com.microfocus.octane.cluster.tasks.api.dto;

import com.microfocus.octane.cluster.tasks.api.enums.CTPPersistStatus;

/**
 * Created by gullery on 16/12/2016.
 *
 * DTO describing the result of single task persistence attempt into the data storage
 */

public interface ClusterTaskPersistenceResult {
	CTPPersistStatus getStatus();
}
