package com.gullerya.cluster.tasks.api;

import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.impl.ClusterTasksProcessorBase;

/**
 * Created by gullery on 08/05/2016.
 * <p>
 * API definition and base implementation of SIMPLE Cluster Tasks Processor
 * - this is the default base class for any simple tasks processors
 */

public abstract class ClusterTasksProcessorSimple extends ClusterTasksProcessorBase {

	protected ClusterTasksProcessorSimple(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode) {
		super(dataProviderType, numberOfWorkersPerNode);
	}

	protected ClusterTasksProcessorSimple(ClusterTasksDataProviderType dataProviderType, int numberOfWorkersPerNode, int minimalTasksTakeInterval) {
		super(dataProviderType, numberOfWorkersPerNode, minimalTasksTakeInterval);
	}
}