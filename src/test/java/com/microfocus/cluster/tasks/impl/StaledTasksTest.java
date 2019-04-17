package com.microfocus.cluster.tasks.impl;

import com.microfocus.cluster.tasks.CTSTestsUtils;
import com.microfocus.cluster.tasks.api.ClusterTasksService;
import com.microfocus.cluster.tasks.api.builders.TaskBuilders;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.cluster.tasks.processors.ClusterTasksStaledTest_A;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Test/s to verify that staled tasks are detected and collected correctly, releasing the queue in cases where concurrency key employed
 */

public class StaledTasksTest {
	private static final Logger logger = LoggerFactory.getLogger(StaledTasksTest.class);

	@Test
	public void TestA_staled_tasks() throws InterruptedException {
		//  load contexts to simulate cluster of a 2 nodes
		CountDownLatch waitForAllInit = new CountDownLatch(2);
		ClassPathXmlApplicationContext staleContext;
		ClassPathXmlApplicationContext pickupContext;
		staleContext = new ClassPathXmlApplicationContext("/staled-tasks-context-test.xml");
		staleContext.getBean(ClusterTasksService.class)
				.getReadyPromise()
				.handleAsync((r, e) -> {
					if (r != null && r) {
						waitForAllInit.countDown();
					} else {
						throw new IllegalStateException("stale context failed to get initialized", e);
					}
					return null;
				});
		pickupContext = new ClassPathXmlApplicationContext("/staled-tasks-context-test.xml");
		pickupContext.getBean(ClusterTasksService.class)
				.getReadyPromise()
				.handleAsync((r, e) -> {
					if (r != null && r) {
						waitForAllInit.countDown();
					} else {
						throw new IllegalStateException("pickup context failed to get initialized", e);
					}
					return null;
				});
		waitForAllInit.await();
		logger.info("2 nodes initialized successfully");

		//  [YG] TODO: do better drain out
		//  let's drain out any old tasks if present
		CTSTestsUtils.waitSafely(4000);
		staleContext.getBean(ClusterTasksStaledTest_A.class).drainMode = false;
		pickupContext.getBean(ClusterTasksStaledTest_A.class).drainMode = false;

		//  make only stale node to pull tasks - it will take 2 tasks per 2 threads
		pickupContext.getBean(ClusterTasksStaledTest_A.class).isStaleRole = false;
		pickupContext.getBean(ClusterTasksStaledTest_A.class).suspended = true;

		//  push 4 tasks, 2 pairs of 2 concurrency keys
		String concurrencyKeyA = UUID.randomUUID().toString().replaceAll("-", "");
		String concurrencyKeyB = UUID.randomUUID().toString().replaceAll("-", "");
		ClusterTask task1 = TaskBuilders.channeledTask().setConcurrencyKey(concurrencyKeyA).build();
		ClusterTask task2 = TaskBuilders.channeledTask().setConcurrencyKey(concurrencyKeyA).build();
		ClusterTask task3 = TaskBuilders.channeledTask().setConcurrencyKey(concurrencyKeyB).build();
		ClusterTask task4 = TaskBuilders.channeledTask().setConcurrencyKey(concurrencyKeyB).build();
		staleContext.getBean(ClusterTasksService.class).enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksStaledTest_A", task1, task2, task3, task4);

		//  wait until the staled node will take 2 tasks (it won't release them until notified)
		CTSTestsUtils.waitUntil(5000, () -> {
			if (staleContext.getBean(ClusterTasksStaledTest_A.class).takenTasksCounter < 2) {
				return null;
			} else {
				return true;
			}
		});

		//  destroy stale node, so that it won't update its activity anymore (actually becoming stale)
		try {
			staleContext.getBean(ClusterTasksService.class).stop()
					.get();
		} catch (Exception e) {
			logger.warn("interrupted while stopping CTS");
		}
		staleContext.close();

		//  wait until the pickup node will remove
		pickupContext.getBean(ClusterTasksServiceImpl.class).getMaintainer().setMaintenanceInterval(2000);
		pickupContext.getBean(ClusterTasksStaledTest_A.class).suspended = false;
		CTSTestsUtils.waitUntil(20000, () -> {
			if (pickupContext.getBean(ClusterTasksStaledTest_A.class).takenTasksCounter < 2) {
				return null;
			} else {
				return true;
			}
		});

		//  clean up the pick up context
		try {
			pickupContext.getBean(ClusterTasksService.class).stop()
					.get();
		} catch (Exception e) {
			logger.warn("interrupted while stopping CTS");
		}
		pickupContext.close();
	}
}
