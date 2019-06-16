/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

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