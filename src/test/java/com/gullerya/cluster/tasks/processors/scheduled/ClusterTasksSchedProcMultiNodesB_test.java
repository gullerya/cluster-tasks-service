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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gullery on 03/03/2019
 * <p>
 * This scheduled tasks processor will serve the test where original interval is 0 and it is redefined with reschedule API
 */

public class ClusterTasksSchedProcMultiNodesB_test extends ClusterTasksProcessorScheduled {
	private static final Object COUNTER_LOCK = new Object();
	private static volatile int executionsCounter = 0;
	public static volatile boolean suspended = true;

	protected ClusterTasksSchedProcMultiNodesB_test() {
		super(ClusterTasksDataProviderType.DB);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (!suspended) {
			synchronized (COUNTER_LOCK) {
				executionsCounter++;
			}
		}
	}

	static public void resetCounter() {
		synchronized (COUNTER_LOCK) {
			executionsCounter = 0;
		}
	}

	static public int getExecutionsCounter() {
		return executionsCounter;
	}
}
