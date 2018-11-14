package com.microfocus.cluster.tasks;

import com.microfocus.cluster.tasks.api.builders.TaskBuilders;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskInsertStatus;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.microfocus.cluster.tasks.processors.ClusterTasksProcessorFairness_test_mt;
import com.microfocus.cluster.tasks.processors.ClusterTasksProcessorFairness_test_st;
import org.junit.Assert;
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

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Collection of integration tests for Cluster Tasks Processor Service to check specifically fairness functionality
 * CTS is expected to give a fair resources to all of the concurrency keys within any (by configuration?) processor
 * All tasks defined with NULL concurrency key should be executed orderly (in FIFO favor) between themselves
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/cluster-tasks-service-context-test.xml"
})
public class ClusterTasksProcessorFairnessTest extends CTSTestsBase {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorFairnessTest.class);

	@Test
	public void TestA_fairness_limited_resource() {
		List<ClusterTask> tasks = new LinkedList<>();
		ClusterTask task;

		//  5 tasks of key '1'
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks.add(task);
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks.add(task);
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks.add(task);
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks.add(task);
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks.add(task);

		//  3 tasks of key '2'
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("2")
				.build();
		tasks.add(task);
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("2")
				.build();
		tasks.add(task);
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("2")
				.build();
		tasks.add(task);

		//  4 tasks without key
		task = TaskBuilders.simpleTask().build();
		tasks.add(task);
		task = TaskBuilders.simpleTask().build();
		tasks.add(task);
		task = TaskBuilders.simpleTask().build();
		tasks.add(task);
		task = TaskBuilders.simpleTask().build();
		tasks.add(task);

		ClusterTaskPersistenceResult[] enqueueResults = clusterTasksService.enqueueTasks(
				ClusterTasksDataProviderType.DB,
				ClusterTasksProcessorFairness_test_st.class.getSimpleName(),
				tasks.toArray(new ClusterTask[0]));
		for (ClusterTaskPersistenceResult result : enqueueResults) {
			Assert.assertEquals(ClusterTaskInsertStatus.SUCCESS, result.getStatus());
		}

		List<String> eventsLog = ClusterTasksProcessorFairness_test_st.keysProcessingEventsLog;
		waitForEndCondition(eventsLog, tasks.size(), tasks.size() * 1000 * 3);

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

		List<Long> nonConcurrentEventsLog = ClusterTasksProcessorFairness_test_st.nonConcurrentEventsLog;
		for (int i = 0; i < nonConcurrentEventsLog.size() - 1; i++) {
			assertTrue(nonConcurrentEventsLog.get(i) <= nonConcurrentEventsLog.get(i + 1));
		}
	}

	//  this test is called to ensure, that given more threads than concurrency keys, dispatcher will occupy all available threads with non-concurrent tasks
	//  due to unpredictable nature of dispatch timing, this test relies only on the fact, that there will be no more than 2 dispatch rounds until for tasks
	//  when in future tasks creation will be batched - we'll be able to test it more thoroughly
	@Test
	public void TestA_fairness_resource_for_multi_non_concurrent() {
		List<ClusterTask> tasks = new LinkedList<>();
		ClusterTask task;

		//  1 tasks of key '1'
		task = TaskBuilders.channeledTask()
				.setConcurrencyKey("1")
				.build();
		tasks.add(task);

		//  3 tasks without key
		task = TaskBuilders.simpleTask().build();
		tasks.add(task);
		task = TaskBuilders.simpleTask().build();
		tasks.add(task);
		task = TaskBuilders.simpleTask().build();
		tasks.add(task);

		ClusterTaskPersistenceResult[] enqueueResults = clusterTasksService.enqueueTasks(
				ClusterTasksDataProviderType.DB,
				ClusterTasksProcessorFairness_test_mt.class.getSimpleName(),
				tasks.toArray(new ClusterTask[0]));
		for (ClusterTaskPersistenceResult result : enqueueResults) {
			Assert.assertEquals(ClusterTaskInsertStatus.SUCCESS, result.getStatus());
		}

		List<String> eventsLog = ClusterTasksProcessorFairness_test_mt.keysProcessingEventsLog;
		waitForEndCondition(eventsLog, tasks.size(), 1000 * 2);

		assertEquals(tasks.size(), eventsLog.size());
	}

	private void waitForEndCondition(List<String> eventStore, int expectedSize, long maxTimeToWait) {
		long timePassed = 0;
		long pauseInterval = 100;
		while (eventStore.size() < expectedSize && timePassed < maxTimeToWait) {
			ClusterTasksTestsUtils.waitSafely(pauseInterval);
			timePassed += pauseInterval;
		}
		if (eventStore.size() == expectedSize) {
			logger.info("expectation fulfilled in " + timePassed + "ms");
			System.out.println("expectation fulfilled in " + timePassed + "ms");
		}
	}
}
