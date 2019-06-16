/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.gullerya.cluster.tasks;

import com.gullerya.cluster.tasks.api.builders.TaskBuilders;
import com.gullerya.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.gullerya.cluster.tasks.api.enums.ClusterTaskInsertStatus;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.processors.ClusterTasksProcessorFairness_test_mt;
import com.gullerya.cluster.tasks.processors.ClusterTasksProcessorFairness_test_st;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Collection of integration tests for Cluster Tasks Processor Service to check specifically fairness functionality
 * CTS is expected to give a fair resources to all of the concurrency keys within any (by configuration?) processor
 * All tasks defined with NULL concurrency key should be executed orderly (in FIFO favor) between themselves
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/cluster-tasks-service-context-test.xml"
})
public class FairnessTest extends CTSTestsBase {

	@Test
	public void testAFairnessLimitedResource() {
		ClusterTask[] tasks = new ClusterTask[12];
		ClusterTask task;

		//  5 tasks of key '1'
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks[0] = task;
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks[1] = task;
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks[2] = task;
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks[3] = task;
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks[4] = task;

		//  3 tasks of key '2'
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("2")
				.build();
		tasks[5] = task;
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("2")
				.build();
		tasks[6] = task;
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("2")
				.build();
		tasks[7] = task;

		//  4 tasks without key
		task = TaskBuilders.simpleTask().build();
		tasks[8] = task;
		task = TaskBuilders.simpleTask().build();
		tasks[9] = task;
		task = TaskBuilders.simpleTask().build();
		tasks[10] = task;
		task = TaskBuilders.simpleTask().build();
		tasks[11] = task;

		ClusterTaskPersistenceResult[] enqueueResults = clusterTasksService.enqueueTasks(
				ClusterTasksDataProviderType.DB,
				ClusterTasksProcessorFairness_test_st.class.getSimpleName(),
				tasks);
		for (ClusterTaskPersistenceResult result : enqueueResults) {
			Assert.assertEquals(ClusterTaskInsertStatus.SUCCESS, result.getStatus());
		}

		List<String> eventsLog = ClusterTasksProcessorFairness_test_st.keysProcessingEventsLog;
		CTSTestsUtils.waitUntil(tasks.length * 2000, () -> eventsLog.size() == tasks.length ? true : null);

		assertEquals(tasks.length, eventsLog.size());
		assertEquals("1", eventsLog.get(0));
		assertEquals("2", eventsLog.get(1));
		assertEquals("null", eventsLog.get(2));
		assertEquals("1", eventsLog.get(3));
		assertEquals("2", eventsLog.get(4));
		assertEquals("null", eventsLog.get(5));
		assertEquals("1", eventsLog.get(6));
		assertEquals("2", eventsLog.get(7));
		assertEquals("null", eventsLog.get(8));
		assertEquals("1", eventsLog.get(9));
		assertEquals("null", eventsLog.get(10));
		assertEquals("1", eventsLog.get(11));

		List<Long> nonConcurrentEventsLog = ClusterTasksProcessorFairness_test_st.nonConcurrentEventsLog;
		for (int i = 0; i < nonConcurrentEventsLog.size() - 1; i++) {
			assertTrue(nonConcurrentEventsLog.get(i) <= nonConcurrentEventsLog.get(i + 1));
		}
	}

	//  this test is called to ensure, that given more threads than concurrency keys, dispatcher will occupy all available threads with non-concurrent tasks
	//  due to unpredictable nature of dispatch timing, this test relies only on the fact, that there will be no more than 2 dispatch rounds until for tasks
	//  when in future tasks creation will be batched - we'll be able to test it more thoroughly
	@Test
	public void testAFairnessResourceForMultiNonConcurrent() {
		List<ClusterTask> tasks = new LinkedList<>();
		ClusterTask task;

		//  1 tasks of key '1'
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks.add(task);

		//  3 tasks without key
		task = TaskBuilders.simpleTask().build();
		tasks.add(task);
		task = TaskBuilders.simpleTask().build();
		tasks.add(task);
		task = TaskBuilders.simpleTask().build();
		tasks.add(task);

		ClusterTaskPersistenceResult[] enqueueResults = clusterTasksService.enqueueTasks(
				ClusterTasksDataProviderType.DB,
				ClusterTasksProcessorFairness_test_mt.class.getSimpleName(),
				tasks.toArray(new ClusterTask[0]));
		for (ClusterTaskPersistenceResult result : enqueueResults) {
			Assert.assertEquals(ClusterTaskInsertStatus.SUCCESS, result.getStatus());
		}

		CTSTestsUtils.waitUntil(3000, () -> ClusterTasksProcessorFairness_test_mt.keysProcessingEventsLog.size() == tasks.size() ? true : null);
	}
}
