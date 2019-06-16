/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.gullerya.cluster.tasks;

import com.gullerya.cluster.tasks.api.ClusterTasksService;
import com.gullerya.cluster.tasks.processors.scheduled.ClusterTasksSchedProcMultiNodesA_test;
import com.gullerya.cluster.tasks.processors.scheduled.ClusterTasksSchedProcMultiNodesB_test;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Collection of integration tests for Cluster Tasks Processor Service to check how the scheduled tasks behaviour in a really clustered environment
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/cluster-tasks-scheduled-processor-multi-nodes-context-test.xml"
})
public class MultiClusterScheduledTasksTest {
	private static final Logger logger = LoggerFactory.getLogger(MultiClusterScheduledTasksTest.class);
	private int numberOfNodes = 3;

	@Autowired
	private ClusterTasksService clusterTasksService;
	@Autowired
	private ClusterTasksSchedProcMultiNodesB_test clusterTasksSchedProcMultiNodes_B_test;

	@Test
	public void testA_scheduled_in_cluster_initial_interval() throws InterruptedException {
		//  load contexts to simulate cluster of a multiple nodes
		CountDownLatch waitForAllInit = new CountDownLatch(numberOfNodes);
		List<ClassPathXmlApplicationContext> contexts = new LinkedList<>();
		ClassPathXmlApplicationContext context;
		for (int i = 0; i < numberOfNodes; i++) {
			context = new ClassPathXmlApplicationContext(
					"/cluster-tasks-scheduled-processor-multi-nodes-context-test.xml"
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
		logger.info(numberOfNodes + " nodes initialized successfully - all scheduled tasks installed");

		//  make sure that the execution of the scheduled tasks is at correct 'speed' regardless of the number of nodes
		ClusterTasksSchedProcMultiNodesA_test.executionsCounter = 0;
		ClusterTasksSchedProcMultiNodesA_test.suspended = false;
		CTSTestsUtils.waitSafely(7000);
		assertTrue(ClusterTasksSchedProcMultiNodesA_test.executionsCounter == 1 || ClusterTasksSchedProcMultiNodesA_test.executionsCounter == 2);

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

	@Test
	public void testB_scheduled_in_cluster_rescheduled() throws Exception {
		//  load contexts to simulate cluster of a multiple nodes
		CountDownLatch waitForAllInit = new CountDownLatch(numberOfNodes);
		List<ClassPathXmlApplicationContext> contexts = new LinkedList<>();
		ClassPathXmlApplicationContext context;
		for (int i = 0; i < numberOfNodes; i++) {
			context = new ClassPathXmlApplicationContext(
					"/cluster-tasks-scheduled-processor-multi-nodes-context-test.xml"
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
		logger.info(numberOfNodes + " nodes initialized successfully - all scheduled tasks installed");

		//  make sure that the execution of the scheduled tasks is at correct 'speed' regardless of the number of nodes
		Assert.assertTrue(clusterTasksService.getReadyPromise().get());
		clusterTasksSchedProcMultiNodes_B_test.reschedule(5000);
		ClusterTasksSchedProcMultiNodesB_test.suspended = true;
		//  let last task to finish
		CTSTestsUtils.waitSafely(100);

		//  zeroize the counter
		synchronized (ClusterTasksSchedProcMultiNodesB_test.executionsCounter) {
			ClusterTasksSchedProcMultiNodesB_test.executionsCounter.set(0);
		}
		ClusterTasksSchedProcMultiNodesB_test.suspended = false;
		CTSTestsUtils.waitSafely(7000);
		assertTrue("unexpected number of executions " + ClusterTasksSchedProcMultiNodesB_test.executionsCounter, ClusterTasksSchedProcMultiNodesB_test.executionsCounter.get() == 1 || ClusterTasksSchedProcMultiNodesB_test.executionsCounter.get() == 2);

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
