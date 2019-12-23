package com.gullerya.cluster.tasks.processors.scheduled;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 03/03/2019
 * <p>
 * This scheduled tasks processor will serve the test where original interval is 0 and it is redefined with reschedule API
 */

public class ClusterTasksSchedProcMultiNodesB_test extends ClusterTasksProcessorScheduled {
	public static ClusterTasksSchedProcMultiNodesB_test instance;
	public static final List<Long> timestamps = Collections.synchronizedList(new LinkedList<>());

	protected ClusterTasksSchedProcMultiNodesB_test() {
		super(ClusterTasksDataProviderType.DB);
		instance = this;
	}

	@Override
	public void processTask(ClusterTask task) {
		timestamps.add(System.currentTimeMillis());
	}
}
