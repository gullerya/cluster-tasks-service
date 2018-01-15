package com.microfocus.octane.cluster.tasks.api;

import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.impl.ClusterTasksProcessorBase;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * API definition and base implementation of DEFAULT/REGULAR Cluster Tasks Processor
 */

public abstract class ClusterTasksProcessorBasic extends ClusterTasksProcessorBase {

	protected ClusterTasksProcessorBasic(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode) {
		super(dataProviderType, numberOfWorkersPerNode);
	}

	protected ClusterTasksProcessorBasic(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode, int minimalTasksTakeInterval) {
		super(dataProviderType, numberOfWorkersPerNode, minimalTasksTakeInterval);
	}
}