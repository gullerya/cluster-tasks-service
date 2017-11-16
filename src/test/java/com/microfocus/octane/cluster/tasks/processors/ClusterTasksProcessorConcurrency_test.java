package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.ClusterTasksITUtils;
import com.microfocus.octane.cluster.tasks.impl.ClusterTaskInternal;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorConcurrency_test extends ClusterTasksProcessorDefault {
	public static String BEAN_ID = "tasksProcessorA_test";

	public int tasksProcessed = 0;

	protected ClusterTasksProcessorConcurrency_test() {
		super(ClusterTasksDataProviderType.DB, 5);
	}

	@Override
	public void processTask(ClusterTaskInternal task) {
		System.out.println("Concurrency Test task '" + task.getBody() + "' started: " + System.currentTimeMillis());
		ClusterTasksITUtils.sleepSafely(5000);
		System.out.println("Concurrency Test task '" + task.getBody() + "' ended: " + System.currentTimeMillis());
		tasksProcessed++;
	}
}
