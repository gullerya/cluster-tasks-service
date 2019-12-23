package com.gullerya.cluster.tasks.processors.scheduled;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.CTSTestsUtils;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksSchedProcDTest extends ClusterTasksProcessorScheduled {
	public static volatile boolean suspended = true;
	public static volatile boolean runAndHold = false;
	public static volatile int executionsCounter = 0;

	protected ClusterTasksSchedProcDTest() {
		super(ClusterTasksDataProviderType.DB);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (!suspended) {
			synchronized (this) {
				executionsCounter++;
			}
		}
		while (runAndHold) {
			CTSTestsUtils.waitSafely(1000);
		}
	}
}
