package com.gullerya.cluster.tasks;

import com.gullerya.cluster.tasks.api.builders.TaskBuilders;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.api.ClusterTasksService;
import com.gullerya.cluster.tasks.processors.ClusterTasksHATest;
import com.gullerya.cluster.tasks.processors.ClusterTasksHCBTest;
import com.gullerya.cluster.tasks.processors.ClusterTasksHCCTest;
import com.gullerya.cluster.tasks.processors.ClusterTasksHCDTest;
import com.gullerya.cluster.tasks.processors.ClusterTasksHCETest;
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
	public void testAHeavyCluster() throws InterruptedException {
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

		ClusterTasksHATest.taskIDs.clear();
		ClusterTasksHCBTest.taskIDs.clear();
		ClusterTasksHCCTest.taskIDs.clear();
		ClusterTasksHCDTest.taskIDs.clear();
		ClusterTasksHCETest.taskIDs.clear();

		//  [YG] TODO: do better drain out
		//  let's drain out any old tasks if present
		CTSTestsUtils.waitSafely(2000);

		assertEquals(0, ClusterTasksHATest.taskIDs.size());
		assertEquals(0, ClusterTasksHCBTest.taskIDs.size());
		assertEquals(0, ClusterTasksHCCTest.taskIDs.size());
		assertEquals(0, ClusterTasksHCDTest.taskIDs.size());
		assertEquals(0, ClusterTasksHCETest.taskIDs.size());
		ClusterTasksHATest.count = true;
		ClusterTasksHCBTest.count = true;
		ClusterTasksHCCTest.count = true;
		ClusterTasksHCDTest.count = true;
		ClusterTasksHCETest.count = true;
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
								.setConcurrencyKey(ClusterTasksHATest.class.getSimpleName() + cnt)
								.setApplicationKey(UUID.randomUUID().toString())
								.setBody(ClusterTasksHATest.CONTENT).build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHATest.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHCBTest.class.getSimpleName() + cnt)
								.setApplicationKey(UUID.randomUUID().toString())
								.setBody(ClusterTasksHCBTest.CONTENT).build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHCBTest.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHCCTest.class.getSimpleName() + cnt)
								.setApplicationKey(UUID.randomUUID().toString())
								.setBody(ClusterTasksHCCTest.CONTENT).build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHCCTest.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHCDTest.class.getSimpleName() + cnt)
								.setApplicationKey(UUID.randomUUID().toString())
								.setBody(ClusterTasksHCDTest.CONTENT).build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHCDTest.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHCETest.class.getSimpleName() + cnt)
								.setApplicationKey(UUID.randomUUID().toString())
								.setBody(ClusterTasksHCETest.CONTENT).build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHCETest.class.getSimpleName(), task);
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
			while (ClusterTasksHATest.taskIDs.size() != numberOfNodes * numberOfTasks) {
				cnt++;
				CTSTestsUtils.waitSafely(100);
				if (cnt % 1000 == 0) {
					logger.info(cnt / 10 + " secs passed, processed " + ClusterTasksHATest.taskIDs.size() + " of " + numberOfNodes * numberOfTasks + " per processor");
				}
			}
			CTSTestsUtils.waitSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHATest.taskIDs.size());
			logger.info("ClusterTasksHATest DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHCBTest.taskIDs.size() != numberOfNodes * numberOfTasks) {
				CTSTestsUtils.waitSafely(100);
			}
			CTSTestsUtils.waitSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHCBTest.taskIDs.size());
			logger.info("ClusterTasksHCBTest DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHCCTest.taskIDs.size() != numberOfNodes * numberOfTasks) {
				CTSTestsUtils.waitSafely(100);
			}
			CTSTestsUtils.waitSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHCCTest.taskIDs.size());
			logger.info("ClusterTasksHCCTest DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHCDTest.taskIDs.size() != numberOfNodes * numberOfTasks) {
				CTSTestsUtils.waitSafely(100);
			}
			CTSTestsUtils.waitSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHCDTest.taskIDs.size());
			logger.info("ClusterTasksHCDTest DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHCETest.taskIDs.size() != numberOfNodes * numberOfTasks) {
				CTSTestsUtils.waitSafely(100);
			}
			CTSTestsUtils.waitSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHCETest.taskIDs.size());
			logger.info("ClusterTasksHCETest DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
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
