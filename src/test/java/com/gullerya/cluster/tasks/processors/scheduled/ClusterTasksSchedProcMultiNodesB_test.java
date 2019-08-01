package com.gullerya.cluster.tasks.processors.scheduled;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gullery on 03/03/2019
 * <p>
 * This scheduled tasks processor will serve the test where original interval is 0 and it is redefined with reschedule API
 */

public class ClusterTasksSchedProcMultiNodesB_test extends ClusterTasksProcessorScheduled {
	public static volatile boolean suspended = true;
	public static final AtomicInteger executionsCounter = new AtomicInteger(0);

	protected ClusterTasksSchedProcMultiNodesB_test() {
		super(ClusterTasksDataProviderType.DB);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (!suspended) {
			executionsCounter.incrementAndGet();
		}
	}
}
