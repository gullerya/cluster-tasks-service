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

/**
 * Created by gullery on 21/11/2018
 */

public class ClusterTasksStaledTest_A extends ClusterTasksProcessorSimple {
	public boolean drainMode = true;
	public boolean suspended = false;
	public boolean isStaleRole = true;
	public final Object MONITOR = new Object();
	public int takenTasksCounter = 0;

	protected ClusterTasksStaledTest_A() {
		super(ClusterTasksDataProviderType.DB, 2);
	}

	@Override
	public void processTask(ClusterTask task) {
		if (drainMode) return;

		synchronized (this) {
			takenTasksCounter++;
		}
		if (isStaleRole) {
			System.out.println("going to stale task");
			synchronized (MONITOR) {
				try {
					MONITOR.wait();
				} catch (InterruptedException ie) {
					System.out.println("interrupted while waiting");
				}
			}
			System.out.println("out of wait");
		} else {
			System.out.println("task taken");
		}
	}

	@Override
	protected boolean isReadyToHandleTasks() {
		return !suspended;
	}
}
