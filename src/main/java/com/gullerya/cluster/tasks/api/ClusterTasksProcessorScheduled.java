package com.gullerya.cluster.tasks.api;

import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;

/**
 * Created by gullery on 15/08/2017.
 * <p>
 * API definition and base implementation of SCHEDULED Cluster Tasks Processor
 * Tasks processors based on ClusterTasksProcessorScheduled class will
 * - should NOT be fed (enqueued) by no tasks from the application side
 * - will automatically handle the full lifecycle of the single dedicated scheduled task (initial creation, running, re-enqueueing etc)
 * - the scheduled task will recur according to the specified run interval
 * - run interval may be defined initially, but also dynamically changed by the concrete tasks processor
 */

public abstract class ClusterTasksProcessorScheduled extends ClusterTasksProcessorSimple {

	protected ClusterTasksProcessorScheduled(ClusterTasksDataProviderType dataProviderType) {
		this(dataProviderType, 0);
	}

	protected ClusterTasksProcessorScheduled(ClusterTasksDataProviderType dataProviderType, long taskRunIntervalMillis) {
		this(dataProviderType, taskRunIntervalMillis, false);
	}

	protected ClusterTasksProcessorScheduled(ClusterTasksDataProviderType dataProviderType, long taskRunIntervalMillis, boolean forceUpdateInterval) {
		super(dataProviderType, 1);
		scheduledTaskRunInterval = Math.max(taskRunIntervalMillis, 0);
		forceUpdateSchedulingInterval = forceUpdateInterval;
	}
}