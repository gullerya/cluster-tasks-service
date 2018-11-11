package com.microfocus.cluster.tasks;

import com.microfocus.cluster.tasks.api.ClusterTasksService;
import com.microfocus.cluster.tasks.api.builders.TaskBuilders;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.cluster.tasks.processors.ClusterTasksHC_A_test;
import com.microfocus.cluster.tasks.processors.ClusterTasksHC_B_test;
import com.microfocus.cluster.tasks.processors.ClusterTasksHC_C_test;
import com.microfocus.cluster.tasks.processors.ClusterTasksHC_D_test;
import com.microfocus.cluster.tasks.processors.ClusterTasksHC_E_test;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

public class ClusterTasksHeavyClusterChanneledTasksTest {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksHeavyClusterChanneledTasksTest.class);
	private int numberOfNodes = 32;
	private int numberOfTasks = 500;

	@Test
	public void TestA_heavy_cluster() throws InterruptedException {
		//  load contexts to simulate cluster of a multiple nodes
		CountDownLatch waitForAllInit = new CountDownLatch(numberOfNodes);
		List<ApplicationContext> contexts = new LinkedList<>();
		ApplicationContext context;
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
		ClusterTasksTestsUtils.sleepSafely(2000);

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
								.setBody("some body to touch the body tables as well").build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHC_A_test.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHC_B_test.class.getSimpleName() + cnt)
								.setBody("some body to touch the body tables as well").build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHC_B_test.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHC_C_test.class.getSimpleName() + cnt)
								.setBody("some body to touch the body tables as well").build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHC_C_test.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHC_D_test.class.getSimpleName() + cnt)
								.setBody("some body to touch the body tables as well").build();
						clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksHC_D_test.class.getSimpleName(), task);

						task = TaskBuilders
								.channeledTask()
								.setConcurrencyKey(ClusterTasksHC_E_test.class.getSimpleName() + cnt)
								.setBody("some body to touch the body tables as well").build();
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
			while (ClusterTasksHC_A_test.taskIDs.size() != numberOfNodes * numberOfTasks) {
				ClusterTasksTestsUtils.sleepSafely(100);
			}
			ClusterTasksTestsUtils.sleepSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHC_A_test.taskIDs.size());
			logger.info("ClusterTasksHC_A_test DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHC_B_test.taskIDs.size() != numberOfNodes * numberOfTasks) {
				ClusterTasksTestsUtils.sleepSafely(100);
			}
			ClusterTasksTestsUtils.sleepSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHC_B_test.taskIDs.size());
			logger.info("ClusterTasksHC_B_test DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHC_C_test.taskIDs.size() != numberOfNodes * numberOfTasks) {
				ClusterTasksTestsUtils.sleepSafely(100);
			}
			ClusterTasksTestsUtils.sleepSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHC_C_test.taskIDs.size());
			logger.info("ClusterTasksHC_C_test DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHC_D_test.taskIDs.size() != numberOfNodes * numberOfTasks) {
				ClusterTasksTestsUtils.sleepSafely(100);
			}
			ClusterTasksTestsUtils.sleepSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHC_D_test.taskIDs.size());
			logger.info("ClusterTasksHC_D_test DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		tasksDonePool.execute(() -> {
			while (ClusterTasksHC_E_test.taskIDs.size() != numberOfNodes * numberOfTasks) {
				ClusterTasksTestsUtils.sleepSafely(100);
			}
			ClusterTasksTestsUtils.sleepSafely(1000);   //  verify no more interactions
			assertEquals(numberOfNodes * numberOfTasks, ClusterTasksHC_E_test.taskIDs.size());
			logger.info("ClusterTasksHC_E_test DONE with " + numberOfNodes * numberOfTasks + " (for all " + numberOfNodes + " nodes)");
			waitForAllTasksDone.countDown();
		});
		waitForAllTasksDone.await();
		long timeToDone = System.currentTimeMillis() - startTime - timeToPush;
		logger.info(numberOfNodes * numberOfTasks * 5 + " tasks has been processed in " + timeToDone + "ms; average of " + ((double) timeToDone / (numberOfNodes * numberOfTasks * 5)) + "ms for task");
	}
}
