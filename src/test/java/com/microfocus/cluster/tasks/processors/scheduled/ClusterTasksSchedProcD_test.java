package com.microfocus.cluster.tasks.processors.scheduled;

import com.microfocus.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksSchedProcD_test extends ClusterTasksProcessorScheduled {
	public static volatile boolean suspended = true;
	public static volatile int executionsCounter = 0;

	protected ClusterTasksSchedProcD_test() {
		super(ClusterTasksDataProviderType.DB);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (!suspended) {
			synchronized (this) {
				executionsCounter++;
			}
		}
	}
}
