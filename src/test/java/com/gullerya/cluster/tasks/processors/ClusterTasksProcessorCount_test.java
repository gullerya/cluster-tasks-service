/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.gullerya.cluster.tasks.processors;

import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.CTSTestsUtils;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorCount_test extends ClusterTasksProcessorSimple {
	public boolean readyToTakeTasks = false;
	public long holdTaskForMillis = 0;

	protected ClusterTasksProcessorCount_test() {
		super(ClusterTasksDataProviderType.DB, 2);
	}

	@Override
	protected boolean isReadyToHandleTasks() {
		return readyToTakeTasks;
	}

	@Override
	public void processTask(ClusterTask task) {
		CTSTestsUtils.waitSafely(holdTaskForMillis);
	}
}
