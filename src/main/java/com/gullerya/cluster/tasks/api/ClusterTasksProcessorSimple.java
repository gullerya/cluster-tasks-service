/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

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