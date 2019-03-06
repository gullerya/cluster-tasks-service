package com.microfocus.cluster.tasks.processors.scheduled;

import com.microfocus.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 03/03/2019
 * <p>
 * This scheduled tasks processor will serve the test where original interval is 0 and it is redefined with reschedule API
 */

public class ClusterTasksSchedProcMultiNodesB_test extends ClusterTasksProcessorScheduled {
	public static volatile boolean suspended = true;
	public static int executionsCounter = 0;

	protected ClusterTasksSchedProcMultiNodesB_test() {
		super(ClusterTasksDataProviderType.DB);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (!suspended) {
			executionsCounter++;
		}
	}
}
