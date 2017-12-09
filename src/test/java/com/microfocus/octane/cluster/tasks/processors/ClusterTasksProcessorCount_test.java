package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.ClusterTasksITUtils;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorCount_test extends ClusterTasksProcessorDefault {
	public boolean readyToTakeTasks = false;
	public long holdTaskForMillis = 0;

	protected ClusterTasksProcessorCount_test() {
		super(ClusterTasksDataProviderType.DB, 2);
	}

	@Override
	protected boolean isReadyToHandleTask() {
		return readyToTakeTasks;
	}

	@Override
	public void processTask(ClusterTask task) {
		ClusterTasksITUtils.sleepSafely(holdTaskForMillis);
	}
}
