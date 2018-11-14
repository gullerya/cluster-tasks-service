package com.microfocus.cluster.tasks.processors;

import com.microfocus.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.cluster.tasks.ClusterTasksTestsUtils;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorConcurrency_test extends ClusterTasksProcessorSimple {
	public int tasksProcessed = 0;

	protected ClusterTasksProcessorConcurrency_test() {
		super(ClusterTasksDataProviderType.DB, 5);
	}

	@Override
	public void processTask(ClusterTask task) {
		System.out.println("Concurrency Test task '" + task.getBody() + "' started: " + System.currentTimeMillis());
		ClusterTasksTestsUtils.waitSafely(5000);
		System.out.println("Concurrency Test task '" + task.getBody() + "' ended: " + System.currentTimeMillis());
		tasksProcessed++;
	}
}
