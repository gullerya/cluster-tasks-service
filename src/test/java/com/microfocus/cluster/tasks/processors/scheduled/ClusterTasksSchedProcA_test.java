package com.microfocus.cluster.tasks.processors.scheduled;

import com.microfocus.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.cluster.tasks.CTSTestsUtils;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksSchedProcA_test extends ClusterTasksProcessorScheduled {
	public static volatile boolean suspended = true;
	public static int executionsCounter = 0;

	protected ClusterTasksSchedProcA_test() {
		super(ClusterTasksDataProviderType.DB);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (!suspended) {
			CTSTestsUtils.waitSafely(1000);
			executionsCounter++;
		}
	}
}
