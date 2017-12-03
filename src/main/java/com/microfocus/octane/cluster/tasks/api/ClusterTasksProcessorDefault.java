package com.microfocus.octane.cluster.tasks.api;

import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.impl.ClusterTasksProcessorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * API definition and base implementation of DEFAULT/REGULAR Cluster Tasks Processor
 */

public abstract class ClusterTasksProcessorDefault extends ClusterTasksProcessorBase {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorDefault.class);

	protected ClusterTasksProcessorDefault(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode) {
		super(dataProviderType, numberOfWorkersPerNode);
	}

	protected ClusterTasksProcessorDefault(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode, int minimalTasksTakeInterval) {
		super(dataProviderType, numberOfWorkersPerNode, minimalTasksTakeInterval);
	}
}