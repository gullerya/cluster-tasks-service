package com.gullerya.cluster.tasks;

import com.gullerya.cluster.tasks.api.builders.TaskBuilders;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTaskStatus;
import com.gullerya.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.gullerya.cluster.tasks.api.enums.ClusterTaskInsertStatus;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.processors.ClusterTasksProcessorUniquenessTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class UniquenessTest extends CTSTestsBase {
	private static final Logger logger = LoggerFactory.getLogger(UniquenessTest.class);

	@Test
	public void uniquenessTestANoConcurrencyKeys() {
		ClusterTask task;
		ClusterTaskPersistenceResult[] results;

		drainTasks();

		//  enqueue first unique task with short delay
		task = TaskBuilders.uniqueTask()
				.setUniquenessKey("task")
				.setDelayByMillis(500)
				.setBody("4000")
				.build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksProcessorUniquenessTest.class.getSimpleName(), task);
		assertEquals(1, results.length);
		assertNotNull(results[0]);
		assertEquals(ClusterTaskInsertStatus.SUCCESS, results[0].getStatus());

		//  attempt to enqueue second task - should fail due to the fact that the first task is still in queue
		task = TaskBuilders.uniqueTask()
				.setUniquenessKey("task")
				.setBody("second")
				.build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksProcessorUniquenessTest.class.getSimpleName(), task);
		assertEquals(1, results.length);
		assertNotNull(results[0]);
		assertEquals(ClusterTaskInsertStatus.UNIQUE_CONSTRAINT_FAILURE, results[0].getStatus());

		//  wait to ensure first task started to run
		CTSTestsUtils.waitSafely(2500);

		//  attempt to enqueue third task - should succeed and due to the first task is already running
		task = TaskBuilders.uniqueTask()
				.setUniquenessKey("task")
				.setBody("0")
				.build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksProcessorUniquenessTest.class.getSimpleName(), task);
		assertEquals(1, results.length);
		assertNotNull(results[0]);
		assertEquals(ClusterTaskInsertStatus.SUCCESS, results[0].getStatus());

		//  attempt to enqueue forth task - should fail due to the fact that the third task is in queue and pending
		task = TaskBuilders.uniqueTask()
				.setUniquenessKey("task")
				.setBody("forth")
				.build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, ClusterTasksProcessorUniquenessTest.class.getSimpleName(), task);
		assertEquals(1, results.length);
		assertNotNull(results[0]);
		assertEquals(ClusterTaskInsertStatus.UNIQUE_CONSTRAINT_FAILURE, results[0].getStatus());

		waitForEndCondition(2, 5000L);
		assertEquals("4000", ClusterTasksProcessorUniquenessTest.bodies.get(0));
		assertEquals("0", ClusterTasksProcessorUniquenessTest.bodies.get(1));

		CTSTestsUtils.waitSafely(1500);
	}

	private void drainTasks() {
		int tasksLeft;
		long maxTimeToWait = 25000L;
		long startTime = System.currentTimeMillis();
		ClusterTasksProcessorUniquenessTest.draining = true;
		while ((tasksLeft = clusterTasksService.countTasks(
				ClusterTasksDataProviderType.DB, ClusterTasksProcessorUniquenessTest.class.getSimpleName(),
				ClusterTaskStatus.PENDING, ClusterTaskStatus.RUNNING)) > 0 &&
				System.currentTimeMillis() - startTime < maxTimeToWait) {
			CTSTestsUtils.waitSafely(300);
		}
		ClusterTasksProcessorUniquenessTest.bodies.clear();
		ClusterTasksProcessorUniquenessTest.draining = false;
		assertEquals(0, tasksLeft);
		System.out.println("tasks drained in " + (System.currentTimeMillis() - startTime));
	}

	private void waitForEndCondition(int expectedSize, long maxTimeToWait) {
		long timePassed = 0;
		long pauseInterval = 100;
		while (ClusterTasksProcessorUniquenessTest.bodies.size() != expectedSize && timePassed < maxTimeToWait) {
			CTSTestsUtils.waitSafely(pauseInterval);
			timePassed += pauseInterval;
		}
		if (ClusterTasksProcessorUniquenessTest.bodies.size() == expectedSize) {
			logger.info("expectation fulfilled in " + timePassed + "ms");
		} else {
			fail("expected to have " + expectedSize + " results, but found " + ClusterTasksProcessorUniquenessTest.bodies.size());
		}
	}
}
