package com.gullerya.cluster.tasks.processors;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorE_test_na extends ClusterTasksProcessorSimple {
	protected ClusterTasksProcessorE_test_na() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	protected boolean isReadyToHandleTasks() {
		return false;
	}

	@Override
	public void processTask(ClusterTask task) {
	}
}
