package com.microfocus.cluster.tasks.processors;

import com.microfocus.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorE_test_na extends ClusterTasksProcessorSimple {
	protected ClusterTasksProcessorE_test_na() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	protected boolean isReadyToHandleTask() {
		return false;
	}

	@Override
	public void processTask(ClusterTask task) {
	}
}
