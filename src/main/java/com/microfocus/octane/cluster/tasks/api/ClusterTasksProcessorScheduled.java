package com.microfocus.octane.cluster.tasks.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gullery on 15/08/2017.
 * <p>
 * API definition and base implementation of SCHEDULED Cluster Tasks Processor
 */

public abstract class ClusterTasksProcessorScheduled extends ClusterTasksProcessorDefault {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorScheduled.class);
	private final long maxTimeToRun;

	protected ClusterTasksProcessorScheduled(ClusterTasksDataProviderType dataProviderType, Long maxTimeToRun) {
		this(dataProviderType, 0, maxTimeToRun);
	}

	protected ClusterTasksProcessorScheduled(ClusterTasksDataProviderType dataProviderType, Integer minimalTasksTakeInterval, Long maxTimeToRun) {
		super(dataProviderType, 1, minimalTasksTakeInterval);
		this.maxTimeToRun = maxTimeToRun;
	}

	public long getMaxTimeToRun() {
		return maxTimeToRun;
	}
}