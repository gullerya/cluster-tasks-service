package com.microfocus.octane.cluster.tasks.processors;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksProcessorDefault;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksHC_D_test extends ClusterTasksProcessorDefault {
	public static volatile int tasksProcessed = 0;

	protected ClusterTasksHC_D_test() {
		super(ClusterTasksDataProviderType.DB, 5);
	}

	@Override
	public void processTask(ClusterTask task) {
		synchronized (this) {
			tasksProcessed++;
		}
	}
}
