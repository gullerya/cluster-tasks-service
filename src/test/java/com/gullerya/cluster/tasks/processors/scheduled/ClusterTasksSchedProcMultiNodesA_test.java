/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

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

public class ClusterTasksSchedProcMultiNodesA_test extends ClusterTasksProcessorScheduled {
	public static volatile boolean suspended = true;
	public static int executionsCounter = 0;
	public static List<Long> executionsIntervals = new LinkedList<>();
	private long lastExecutionTime = 0;

	protected ClusterTasksSchedProcMultiNodesA_test() {
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
