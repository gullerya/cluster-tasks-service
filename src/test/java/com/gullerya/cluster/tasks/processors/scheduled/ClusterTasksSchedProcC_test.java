package com.gullerya.cluster.tasks.processors.scheduled;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.CTSTestsUtils;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksSchedProcC_test extends ClusterTasksProcessorScheduled {
	public static volatile boolean suspended = true;
	public static int executionsCounter = 0;

	protected ClusterTasksSchedProcC_test() {
		super(ClusterTasksDataProviderType.DB);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (!suspended) {
			CTSTestsUtils.waitSafely(3000);
			executionsCounter++;
		}
	}
}
