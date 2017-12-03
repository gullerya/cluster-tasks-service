package com.microfocus.octane.cluster.tasks;

import com.microfocus.octane.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.microfocus.octane.cluster.tasks.api.dto.TaskToEnqueue;
import com.microfocus.octane.cluster.tasks.api.enums.CTPPersistStatus;
import com.microfocus.octane.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.processors.ClusterTasksProcessorFairness_test_SingleThread;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Collection of integration tests for Cluster Tasks Processor Service to check specifically fairness functionality
 * CTS is expected to give a fair resources to all of the concurrency keys within any (by configuration?) processor
 * All tasks defined with NULL concurrency key should be executed orderly (in FIFO favor) between themselves
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/cluster-tasks-processor-context-test.xml",
		"classpath*:/SpringIOC/**/*.xml"
})
public class ClusterTasksProcessorFairnessTest extends CTSTestsBase {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorFairnessTest.class);

	@Test
	public void TestA_fairness_limited_resource() {
		List<TaskToEnqueue> tasks = new LinkedList<>();
		TaskToEnqueue task;

		//  5 tasks of key '1'
		task = new TaskToEnqueue();
		task.setConcurrencyKey("1");
		tasks.add(task);
		task = new TaskToEnqueue();
		task.setConcurrencyKey("1");
		tasks.add(task);
		task = new TaskToEnqueue();
		task.setConcurrencyKey("1");
		tasks.add(task);
		task = new TaskToEnqueue();
		task.setConcurrencyKey("1");
		tasks.add(task);
		task = new TaskToEnqueue();
		task.setConcurrencyKey("1");
		tasks.add(task);

		//  3 tasks of key '2'
		task = new TaskToEnqueue();
		task.setConcurrencyKey("2");
		tasks.add(task);
		task = new TaskToEnqueue();
		task.setConcurrencyKey("2");
		tasks.add(task);
		task = new TaskToEnqueue();
		task.setConcurrencyKey("2");
		tasks.add(task);

		//  4 tasks without key
		task = new TaskToEnqueue();
		tasks.add(task);
		task = new TaskToEnqueue();
		tasks.add(task);
		task = new TaskToEnqueue();
		tasks.add(task);
		task = new TaskToEnqueue();
		tasks.add(task);

		ClusterTaskPersistenceResult[] enqueueResults = clusterTasksService.enqueueTasks(
				ClusterTasksDataProviderType.DB,
				ClusterTasksProcessorFairness_test_SingleThread.class.getSimpleName(),
				tasks.toArray(new TaskToEnqueue[tasks.size()]));
		for (ClusterTaskPersistenceResult result : enqueueResults) {
			if (result.getStatus() != CTPPersistStatus.SUCCESS) fail("failed to enqueue tasks");
		}

		waitForEndCondition(tasks.size(), tasks.size() * 1000 * 3);

		List<String> eventsLog = ClusterTasksProcessorFairness_test_SingleThread.keysProcessingEventsLog;
		assertEquals(tasks.size(), eventsLog.size());
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

		List<Long> nonConcurrentEventsLog = ClusterTasksProcessorFairness_test_SingleThread.nonConcurrentEventsLog;
		for (int i = 0; i < nonConcurrentEventsLog.size() - 1; i++) {
			assertTrue(nonConcurrentEventsLog.get(i) <= nonConcurrentEventsLog.get(i + 1));
		}
	}

	@Test
	public void TestA_fairness_resource_for_multi_non_concurrent() {
		List<TaskToEnqueue> tasks = new LinkedList<>();
		TaskToEnqueue task;


	}

	private void waitForEndCondition(int expectedSize, long maxTimeToWait) {
		long timePassed = 0;
		long pauseInterval = 439;
		while (ClusterTasksProcessorFairness_test_SingleThread.keysProcessingEventsLog.size() < expectedSize && timePassed < maxTimeToWait) {
			ClusterTasksITUtils.sleepSafely(pauseInterval);
			timePassed += pauseInterval;
		}
		if (ClusterTasksProcessorFairness_test_SingleThread.keysProcessingEventsLog.size() == expectedSize) {
			logger.info("expectation fulfilled in " + timePassed + "ms");
			System.out.println("expectation fulfilled in " + timePassed + "ms");
		}
	}
}
