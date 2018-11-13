package com.microfocus.cluster.tasks;

import com.microfocus.cluster.tasks.api.builders.TaskBuilders;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.cluster.tasks.api.ClusterTasksService;
import com.microfocus.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.microfocus.cluster.tasks.api.enums.CTPPersistStatus;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.cluster.tasks.processors.ClusterTasksProcessorUniqueness_test;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Collection of integration tests for Cluster Tasks Processor Service to check specifically concurrency functionality
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/cluster-tasks-service-context-test.xml"
})
public class ClusterTasksProcessorUniquenessTest extends CTSTestsBase {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorUniquenessTest.class);

	@Autowired
	private ClusterTasksService clusterTasksService;

	@Test
	public void UniquenessTest_A_no_concurrency_keys() {
		ClusterTask task;
		ClusterTaskPersistenceResult[] results;

		drainTasks();

		//  enqueue first unique task with short delay
		task = TaskBuilders.uniqueTask()
				.setUniquenessKey("task")
				.setDelayByMillis(500L)
				.setBody("4000")
				.build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksProcessorUniqueness_test.class.getSimpleName(), task);
		assertEquals(1, results.length);
		assertNotNull(results[0]);
		assertEquals(CTPPersistStatus.SUCCESS, results[0].getStatus());

		//  attempt to enqueue second task - should fail due to the fact that the first task is still in queue
		task = TaskBuilders.uniqueTask()
				.setUniquenessKey("task")
				.setBody("second")
				.build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksProcessorUniqueness_test.class.getSimpleName(), task);
		assertEquals(1, results.length);
		assertNotNull(results[0]);
		assertEquals(CTPPersistStatus.UNIQUE_CONSTRAINT_FAILURE, results[0].getStatus());

		//  wait to ensure first task started to run
		ClusterTasksTestsUtils.sleepSafely(2500);

		//  attempt to enqueue third task - should succeed and due to the first task is already running
		task = TaskBuilders.uniqueTask()
				.setUniquenessKey("task")
				.setBody("0")
				.build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksProcessorUniqueness_test.class.getSimpleName(), task);
		assertEquals(1, results.length);
		assertNotNull(results[0]);
		assertEquals(CTPPersistStatus.SUCCESS, results[0].getStatus());

		//  attempt to enqueue forth task - should fail due to the fact that the third task is in queue and pending
		task = TaskBuilders.uniqueTask()
				.setUniquenessKey("task")
				.setBody("forth")
				.build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksProcessorUniqueness_test.class.getSimpleName(), task);
		assertEquals(1, results.length);
		assertNotNull(results[0]);
		assertEquals(CTPPersistStatus.UNIQUE_CONSTRAINT_FAILURE, results[0].getStatus());

		waitForEndCondition(2, 5000L);
		assertEquals("4000", ClusterTasksProcessorUniqueness_test.bodies.get(0));
		assertEquals("0", ClusterTasksProcessorUniqueness_test.bodies.get(1));

		ClusterTasksTestsUtils.sleepSafely(1500);
	}

	private void drainTasks() {
		int tasksLeft;
		long maxTimeToWait = 25000L;
		long startTime = System.currentTimeMillis();
		ClusterTasksProcessorUniqueness_test.draining = true;
		while ((tasksLeft = clusterTasksService.countTasks(
				ClusterTasksDataProviderType.DB, ClusterTasksProcessorUniqueness_test.class.getSimpleName(),
				ClusterTaskStatus.PENDING, ClusterTaskStatus.RUNNING)) > 0 &&
				System.currentTimeMillis() - startTime < maxTimeToWait) {
			ClusterTasksTestsUtils.sleepSafely(300);
		}
		ClusterTasksProcessorUniqueness_test.bodies.clear();
		ClusterTasksProcessorUniqueness_test.draining = false;
		assertEquals(0, tasksLeft);
		System.out.println("tasks drained in " + (System.currentTimeMillis() - startTime));
	}

	private void waitForEndCondition(int expectedSize, long maxTimeToWait) {
		long timePassed = 0;
		long pauseInterval = 100;
		while (ClusterTasksProcessorUniqueness_test.bodies.size() != expectedSize && timePassed < maxTimeToWait) {
			ClusterTasksTestsUtils.sleepSafely(pauseInterval);
			timePassed += pauseInterval;
		}
		if (ClusterTasksProcessorUniqueness_test.bodies.size() == expectedSize) {
			logger.info("expectation fulfilled in " + timePassed + "ms");
			System.out.println("expectation fulfilled in " + timePassed + "ms");
		} else {
			fail("expected to have " + expectedSize + " results, but found " + ClusterTasksProcessorUniqueness_test.bodies.size());
		}
	}
}
