package com.microfocus.octane.cluster.tasks.processors.scheduled;

import com.microfocus.octane.cluster.tasks.ClusterTasksITUtils;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksSchedProcC_test extends ClusterTasksProcessorScheduled {
	public static volatile boolean suspended = true;
	public static int tasksCounter = 0;

	protected ClusterTasksSchedProcC_test() {
		super(ClusterTasksDataProviderType.DB, 12000L);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (!suspended) {
			ClusterTasksITUtils.sleepSafely(3000);
			tasksCounter++;
		}
	}
}
