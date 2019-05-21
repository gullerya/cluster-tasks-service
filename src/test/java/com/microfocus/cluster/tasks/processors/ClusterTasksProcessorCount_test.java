package com.microfocus.cluster.tasks.processors;

import com.microfocus.cluster.tasks.CTSTestsUtils;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.cluster.tasks.api.ClusterTasksProcessorSimple;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorCount_test extends ClusterTasksProcessorSimple {
	public boolean readyToTakeTasks = false;
	public long holdTaskForMillis = 0;

	protected ClusterTasksProcessorCount_test() {
		super(ClusterTasksDataProviderType.DB, 2);
	}

	@Override
	protected boolean isReadyToHandleTasks() {
		return readyToTakeTasks;
	}

	@Override
	public void processTask(ClusterTask task) {
		CTSTestsUtils.waitSafely(holdTaskForMillis);
	}
}
