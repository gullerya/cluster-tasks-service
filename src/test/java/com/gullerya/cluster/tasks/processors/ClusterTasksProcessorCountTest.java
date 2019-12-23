package com.gullerya.cluster.tasks.processors;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.CTSTestsUtils;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorCountTest extends ClusterTasksProcessorSimple {
	public boolean readyToTakeTasks = false;
	public long holdTaskForMillis = 0;

	protected ClusterTasksProcessorCountTest() {
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
