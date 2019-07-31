/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.gullerya.cluster.tasks.processors;

import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.api.ClusterTasksProcessorSimple;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gullery on 02/06/2016
 */

public class ClusterTasksProcessorFairness_test_st extends ClusterTasksProcessorSimple {
	public static List<String> keysProcessingEventsLog = new LinkedList<>();
	public static List<Long> nonConcurrentEventsLog = new LinkedList<>();

	protected ClusterTasksProcessorFairness_test_st() {
		super(ClusterTasksDataProviderType.DB, 1);
	}

	@Override
	public void processTask(ClusterTask task) {
		keysProcessingEventsLog.add(String.valueOf(task.getConcurrencyKey()));
		if (task.getConcurrencyKey() == null) {
			nonConcurrentEventsLog.add(task.getOrderingFactor());
		}
	}
}