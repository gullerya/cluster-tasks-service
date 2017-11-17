package com.microfocus.octane.cluster.tasks;

import com.microfocus.octane.cluster.tasks.api.dto.TaskToEnqueue;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.processors.ClusterTasksProcessorConcurrency_test;
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
		"/cluster-tasks-processor-context-test.xml",
		"classpath*:/SpringIOC/**/*.xml"
})
public class ClusterTasksProcessorConcurrencyTest extends CTSTestsBase {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorConcurrencyTest.class);

	@Autowired
	private ClusterTasksProcessorConcurrency_test clusterTasksProcessorConcurrency_test;

	@Test
	public void TestA_concurrency_value_all_null() {
		TaskToEnqueue[] tasks = new TaskToEnqueue[2];
		tasks[0] = new TaskToEnqueue();
		tasks[0].setBody("test A - task 1 - concurrency value is NULL");
		tasks[1] = new TaskToEnqueue();
		tasks[1].setBody("test A - task 2 - concurrency value is NULL");

		clusterTasksProcessorConcurrency_test.tasksProcessed = 0;
		clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorConcurrency_test", tasks);
		waitForEndCondition(2, 7000);

		assertEquals(2, clusterTasksProcessorConcurrency_test.tasksProcessed);
	}

	@Test
	public void TestB_concurrency_value_some_null() {
		String concurrencyKeyA = UUID.randomUUID().toString();
		String concurrencyKeyB = UUID.randomUUID().toString();
		TaskToEnqueue[] tasks = new TaskToEnqueue[6];
		tasks[0] = new TaskToEnqueue();
		tasks[0].setConcurrencyKey(concurrencyKeyA);
		tasks[0].setBody("test B - task 1 - concurrency value is " + concurrencyKeyA);
		tasks[1] = new TaskToEnqueue();
		tasks[1].setBody("test B - task 2 - concurrency value is NULL");
		tasks[2] = new TaskToEnqueue();
		tasks[2].setConcurrencyKey(concurrencyKeyB);
		tasks[2].setBody("test B - task 3 - concurrency value in " + concurrencyKeyB);
		tasks[3] = new TaskToEnqueue();
		tasks[3].setConcurrencyKey(concurrencyKeyA);
		tasks[3].setBody("test B - task 4 - concurrency value is " + concurrencyKeyA);
		tasks[4] = new TaskToEnqueue();
		tasks[4].setBody("test B - task 5 - concurrency value is NULL");
		tasks[5] = new TaskToEnqueue();
		tasks[5].setConcurrencyKey(concurrencyKeyB);
		tasks[5].setBody("test B - task 6 - concurrency value in " + concurrencyKeyB);

		clusterTasksProcessorConcurrency_test.tasksProcessed = 0;
		clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorConcurrency_test", tasks);
		waitForEndCondition(6, 14000);

		assertEquals(6, clusterTasksProcessorConcurrency_test.tasksProcessed);
	}

	private void waitForEndCondition(int expectedSize, long maxTimeToWait) {
		long timePassed = 0;
		long pauseInterval = 439;
		while (clusterTasksProcessorConcurrency_test.tasksProcessed != expectedSize && timePassed < maxTimeToWait) {
			ClusterTasksITUtils.sleepSafely(pauseInterval);
			timePassed += pauseInterval;
		}
		if (clusterTasksProcessorConcurrency_test.tasksProcessed == expectedSize) {
			logger.info("expectation fulfilled in " + timePassed + "ms");
			System.out.println("expectation fulfilled in " + timePassed + "ms");
		}
	}
}
