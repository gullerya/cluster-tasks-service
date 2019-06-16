/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.gullerya.cluster.tasks.simple;

import com.gullerya.cluster.tasks.api.builders.TaskBuilders;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.CTSTestsBase;
import com.gullerya.cluster.tasks.CTSTestsUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Collection of integration tests for Cluster Tasks Processor Service to check specifically fairness functionality
 * CTS is expected to give a fair resources to all of the concurrency keys within any (by configuration?) processor
 * All tasks defined with NULL concurrency key should be executed orderly (in FIFO favor) between themselves
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/simple-tasks-tests-context.xml"
})
public class SimpleTasksBasicTest extends CTSTestsBase {

	@Test
	public void testASimpleTasksBurst() {
		int numberOfTasks = 20;
		CTSTestsUtils.waitSafely(4000);
		SimpleProcessorA_test.tasksProcessed.clear();

		ClusterTask[] tasks = new ClusterTask[numberOfTasks];

		for (int i = 0; i < numberOfTasks; i++) {
			ClusterTask tmp = TaskBuilders.simpleTask()
					.setBody(String.valueOf(i))
					.build();
			tasks[i] = tmp;
		}

		Assert.assertTrue(SimpleProcessorA_test.tasksProcessed.isEmpty());
		clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "SimpleProcessorA_test", tasks);
		CTSTestsUtils.waitUntil(30000, () -> SimpleProcessorA_test.tasksProcessed.size() == numberOfTasks ? true : null);

		//  verify that all were processed
		for (int i = 0; i < numberOfTasks; i++) {
			Assert.assertTrue(SimpleProcessorA_test.tasksProcessed.containsKey(String.valueOf(i)));
		}

		//  verify an expected order of execution
		for (int i = 0; i < numberOfTasks - 1; i++) {
			Assert.assertTrue(
					"expected positive diff, but got: " + (SimpleProcessorA_test.tasksProcessed.get(String.valueOf(i + 1)) - SimpleProcessorA_test.tasksProcessed.get(String.valueOf(i)) + " between (later) " + (i + 1) + " and " + i),
					SimpleProcessorA_test.tasksProcessed.get(String.valueOf(i + 1)) > SimpleProcessorA_test.tasksProcessed.get(String.valueOf(i)));
		}
	}
}
