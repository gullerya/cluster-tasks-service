/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.gullerya.cluster.tasks;

import com.gullerya.cluster.tasks.api.builders.TaskBuilders;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.api.ClusterTasksService;
import com.gullerya.cluster.tasks.processors.ClusterTasksHC_A_test;
import com.gullerya.cluster.tasks.processors.ClusterTasksHC_B_test;
import com.gullerya.cluster.tasks.processors.ClusterTasksHC_C_test;
import com.gullerya.cluster.tasks.processors.ClusterTasksHC_D_test;
import com.gullerya.cluster.tasks.processors.ClusterTasksHC_E_test;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/**
 * Created by gullery on 04/11/2018.
 * <p>
 * Collection of integration tests for Cluster Tasks Processor Service to check how the whole system performs in a 'heavy' cluster and load simulation
 * - we are raising a number nodes as specified below
 * - we are creating many CHANNELED tasks
 * - we are creating few regular tasks processor and push the specified amount of tasks for each of those
 * - tasks' processor will do nothing except incrementing the counters, so the only time consuming logic here is the own CTS logic
 * - we will than be measuring the time when all of the tasks got drained
 */

public class HeavyClusterChanneledTasksTest {
	private static final Logger logger = LoggerFactory.getLogger(HeavyClusterChanneledTasksTest.class);
	private int numberOfNodes = 32;
	private int numberOfTasks = 200;

	@Test
	public void TestA_heavy_cluster() throws InterruptedException {
		//  load contexts to simulate cluster of a multiple nodes
		CountDownLatch waitForAllInit = new CountDownLatch(numberOfNodes);
		List<ClassPathXmlApplicationContext> contexts = new LinkedList<>();
		ClassPathXmlApplicationContext context;
		for (int i = 0; i < numberOfNodes; i++) {
			context = new ClassPathXmlApplicationContext(
					"/cluster-tasks-heavy-cluster-context-test.xml"
			);
			contexts.add(context);
			context.getBean(ClusterTasksService.class)
					.getReadyPromise()
					.handleAsync((r, e) -> {
						if (r != null && r) {
							waitForAllInit.countDown();
						} else {
							throw new IllegalStateException("some of the contexts failed to get initialized", e);
						}
						return null;
					});
		}
		waitForAllInit.await();
		logger.info(numberOfNodes + " nodes initialized successfully");

		ClusterTasksHC_A_test.taskIDs.clear();
		ClusterTasksHC_B_test.taskIDs.clear();
		ClusterTasksHC_C_test.taskIDs.clear();
		ClusterTasksHC_D_test.taskIDs.clear();
		ClusterTasksHC_E_test.taskIDs.clear();

		//  [YG] TODO: do better drain out
		//  let's drain out any old tasks if present
		CTSTestsUtils.waitSafely(2000);

		assertEquals(0, ClusterTasksHC_A_test.taskIDs.size());
		assertEquals(0, ClusterTasksHC_B_test.taskIDs.size());
		assertEquals(0, ClusterTasksHC_C_test.taskIDs.size());
		assertEquals(0, ClusterTasksHC_D_test.taskIDs.size());
		assertEquals(0, ClusterTasksHC_E_test.taskIDs.size());
		ClusterTasksHC_A_test.count = true;
		ClusterTasksHC_B_test.count = true;
		ClusterTasksHC_C_test.count = true;
		ClusterTasksHC_D_test.count = true;
		ClusterTasksHC_E_test.count = true;
		long startTime = System.currentTimeMillis();

		//  enqueue tasks for all of the contexts
		CountDownLatch waitForAllTasksPush = new CountDownLatch(numberOfNodes);
		Map.Entry<Integer, Integer> tmp = new AbstractMap.SimpleEntry<>(0, 0);
		ExecutorService tasksPushPool = Executors.newFixedThreadPool(numberOfNodes);
		for (int i = 0; i < contexts.size(); i++) {
			ApplicationContext c = contexts.get(i);
			tmp.setValue(i);
			int cnt = i;
			tasksPushPool.execute(() -> {
				try {
					for (int j = 0; j < numberOfTasks; j++) {
						ClusterTasksService clusterTasksService = c.getBean(ClusterTasksService.class);
						ClusterTask task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHC_A_test.class.getSimpleName() + cnt)
								.setApplicationKey(UUID.randomUUID().toString())
								.setBody(ClusterTasksHC_A_test.CONTENT).build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHC_A_test.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHC_B_test.class.getSimpleName() + cnt)
								.setApplicationKey(UUID.randomUUID().toString())
								.setBody(ClusterTasksHC_B_test.CONTENT).build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHC_B_test.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHC_C_test.class.getSimpleName() + cnt)
								.setApplicationKey(UUID.randomUUID().toString())
								.setBody(ClusterTasksHC_C_test.CONTENT).build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHC_C_test.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHC_D_test.class.getSimpleName() + cnt)
								.setApplicationKey(UUID.randomUUID().toString())
								.setBody(ClusterTasksHC_D_test.CONTENT).build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHC_D_test.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHC_E_test.class.getSimpleName() + cnt)
								.setApplicationKey(UUID.randomUUID().toString())
								.setBody(ClusterTasksHC_E_test.CONTENT).build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHC_E_test.class.getSimpleName(), task);
					}
				} catch (Exception e) {
					logger.error("one of the nodes' task push failed", e);
				} finally {
					logger.info("one of the nodes done with tasks push");
					waitForAllTasksPush.countDown();
				}
			});
		}
		waitForAllTasksPush.await();
		long timeToPush = System.currentTimeMillis() - startTime;
		logger.info(numberOfNodes * numberOfTasks * 5 + " tasks has been pushed in " + timeToPush + "ms; average of " + ((double) timeToPush / (numberOfNodes * numberOfTasks * 5)) + "ms for task");

		//  wait for all tasks to be drained
		CountDownLatch waitForAllTasksDone = new CountDownLatch(5);
		ExecutorService tasksDonePool = Executors.newFixedThreadPool(5);
		tasksDonePool.execute(() -> {
			int cnt = 0;
			while (ClusterTasksHC_A_test.taskIDs.size() != numberOfNodes * numberOfTasks) {
				cnt++;
				CTSTestsUtils.waitSafely(100);
				if (cnt % 1000 == 0) {
					logger.info(cnt / 10 + " secs passed, processed " + ClusterTasksHC_A_test.taskIDs.size() + " of " + numberOfNodes * numberOfTasks + " per processor");
				}
			}
			CTSTestsUtils.waitSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHC_A_test.taskIDs.size());
			logger.info("ClusterTasksHC_A_test DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHC_B_test.taskIDs.size() != numberOfNodes * numberOfTasks) {
				CTSTestsUtils.waitSafely(100);
			}
			CTSTestsUtils.waitSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHC_B_test.taskIDs.size());
			logger.info("ClusterTasksHC_B_test DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHC_C_test.taskIDs.size() != numberOfNodes * numberOfTasks) {
				CTSTestsUtils.waitSafely(100);
			}
			CTSTestsUtils.waitSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHC_C_test.taskIDs.size());
			logger.info("ClusterTasksHC_C_test DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHC_D_test.taskIDs.size() != numberOfNodes * numberOfTasks) {
				CTSTestsUtils.waitSafely(100);
			}
			CTSTestsUtils.waitSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHC_D_test.taskIDs.size());
			logger.info("ClusterTasksHC_D_test DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHC_E_test.taskIDs.size() != numberOfNodes * numberOfTasks) {
				CTSTestsUtils.waitSafely(100);
			}
			CTSTestsUtils.waitSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHC_E_test.taskIDs.size());
			logger.info("ClusterTasksHC_E_test DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		waitForAllTasksDone.await();
		long timeToDone = System.currentTimeMillis() - startTime;
		logger.info(numberOfNodes * numberOfTasks * 5 + " tasks has been processed in " + timeToDone + "ms; average of " + ((double) timeToDone / (numberOfNodes * numberOfTasks * 5)) + "ms for task");

		//  stop all CTS instances
		contexts.forEach(c -> {
			try {
				c.getBean(ClusterTasksService.class).stop()
						.get();
			} catch (Exception e) {
				logger.warn("interrupted while stopping CTS");
			}
			c.close();
		});
	}
}
