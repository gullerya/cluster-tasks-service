package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.ClusterTasksITUtils;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorBasic;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorConcurrency_test extends ClusterTasksProcessorBasic {
	public int tasksProcessed = 0;

	protected ClusterTasksProcessorConcurrency_test() {
		super(ClusterTasksDataProviderType.DB, 5);
	}

	@Override
	public void processTask(ClusterTask task) {
		System.out.println("Concurrency Test task '" + task.getBody() + "' started: " + System.currentTimeMillis());
		ClusterTasksITUtils.sleepSafely(5000);
		System.out.println("Concurrency Test task '" + task.getBody() + "' ended: " + System.currentTimeMillis());
		tasksProcessed++;
	}
}
