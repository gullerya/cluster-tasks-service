package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksHC_D_test extends ClusterTasksProcessorDefault {
	private static final Object COUNT_LOCK = new Object();
	public static volatile int tasksProcessed = 0;
	public static volatile boolean count = false;

	protected ClusterTasksHC_D_test() {
		super(ClusterTasksDataProviderType.DB, 5);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (count) {
			synchronized (COUNT_LOCK) {
				tasksProcessed++;
			}
		}
	}
}
