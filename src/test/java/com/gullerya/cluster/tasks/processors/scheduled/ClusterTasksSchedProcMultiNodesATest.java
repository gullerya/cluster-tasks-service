package com.gullerya.cluster.tasks.processors.scheduled;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorScheduled;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 03/03/2019
 * <p>
 * This scheduled tasks processor will serve the test where originally defined interval is tested
 */

public class ClusterTasksSchedProcMultiNodesATest extends ClusterTasksProcessorScheduled {
	public static volatile boolean suspended = true;
	public static int executionsCounter = 0;
	public static List<Long> executionsIntervals = new LinkedList<>();
	private long lastExecutionTime = 0;

	protected ClusterTasksSchedProcMultiNodesATest() {
		super(ClusterTasksDataProviderType.DB, 5000, true);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (!suspended) {
			executionsCounter++;
			if (lastExecutionTime > 0) {
				executionsIntervals.add(System.currentTimeMillis() - lastExecutionTime);
			}
			lastExecutionTime = System.currentTimeMillis();
		}
	}
}
