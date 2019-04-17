package com.microfocus.cluster.tasks;

import com.microfocus.cluster.tasks.api.builders.TaskBuilders;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.cluster.tasks.processors.ClusterTasksProcessorConcurrency_test;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Collection of integration tests for Cluster Tasks Processor Service to check specifically concurrency functionality
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/cluster-tasks-service-context-test.xml"
})
public class ConcurrencyTest extends CTSTestsBase {
	private static final Logger logger = LoggerFactory.getLogger(ConcurrencyTest.class);

	@Autowired
	private ClusterTasksProcessorConcurrency_test clusterTasksProcessorConcurrency_test;

	@Test
	public void testA_concurrency_value_all_null() {
		ClusterTask[] tasks = new ClusterTask[2];
		tasks[0] = TaskBuilders.simpleTask()
				.setBody("test A - task 1 - concurrency value is NULL")
				.build();
		tasks[1] = TaskBuilders.simpleTask()
				.setBody("test A - task 2 - concurrency value is NULL")
				.build();

		clusterTasksProcessorConcurrency_test.tasksProcessed = 0;
		clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorConcurrency_test", tasks);
		waitForEndCondition(2, 7000);

		assertEquals(2, clusterTasksProcessorConcurrency_test.tasksProcessed);
	}

	@Test
	public void testB_concurrency_value_some_null() {
		String concurrencyKeyA = UUID.randomUUID().toString().replaceAll("-", "");
		String concurrencyKeyB = UUID.randomUUID().toString().replaceAll("-", "");
		ClusterTask[] tasks = new ClusterTask[6];
		tasks[0] = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKeyA)
				.setBody("test B - task 1 - concurrency value is " + concurrencyKeyA)
				.build();
		tasks[1] = TaskBuilders.simpleTask()
				.setBody("test B - task 2 - concurrency value is NULL")
				.build();
		tasks[2] = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKeyB)
				.setBody("test B - task 3 - concurrency value in " + concurrencyKeyB)
				.build();
		tasks[3] = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKeyA)
				.setBody("test B - task 4 - concurrency value is " + concurrencyKeyA)
				.build();
		tasks[4] = TaskBuilders.simpleTask()
				.setBody("test B - task 5 - concurrency value is NULL")
				.build();
		tasks[5] = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKeyB)
				.setBody("test B - task 6 - concurrency value in " + concurrencyKeyB)
				.build();

		clusterTasksProcessorConcurrency_test.tasksProcessed = 0;
		clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorConcurrency_test", tasks);
		waitForEndCondition(6, 14000);

		assertEquals(6, clusterTasksProcessorConcurrency_test.tasksProcessed);
	}

	private void waitForEndCondition(int expectedSize, long maxTimeToWait) {
		long timePassed = 0;
		long pauseInterval = 100;
		while (clusterTasksProcessorConcurrency_test.tasksProcessed != expectedSize && timePassed < maxTimeToWait) {
			CTSTestsUtils.waitSafely(pauseInterval);
			timePassed += pauseInterval;
		}
		if (clusterTasksProcessorConcurrency_test.tasksProcessed == expectedSize) {
			logger.info("expectation fulfilled in " + timePassed + "ms");
		}
	}
}
