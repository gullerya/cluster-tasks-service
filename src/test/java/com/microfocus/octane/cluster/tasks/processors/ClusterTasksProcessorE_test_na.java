package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorBasic;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorE_test_na extends ClusterTasksProcessorBasic {
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
