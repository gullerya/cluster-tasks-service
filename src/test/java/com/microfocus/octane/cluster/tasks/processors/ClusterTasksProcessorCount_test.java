package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.ClusterTasksITUtils;
import com.microfocus.octane.cluster.tasks.impl.ClusterTaskInternal;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorCount_test extends ClusterTasksProcessorDefault {
	public static String BEAN_ID = "tasksProcessorCount_test";
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
	public void processTask(ClusterTaskInternal task) {
		ClusterTasksITUtils.sleepSafely(holdTaskForMillis);
	}
}
