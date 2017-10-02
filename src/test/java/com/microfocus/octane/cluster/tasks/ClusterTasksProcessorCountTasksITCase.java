package com.microfocus.octane.cluster.tasks;

import com.microfocus.octane.cluster.tasks.api.CTPPersistStatus;
import com.microfocus.octane.cluster.tasks.api.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.ClusterTaskPersistenceResult;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.processors.ClusterTasksProcessorCount_test;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Main collection of integration tests for Cluster Tasks Processor - Count Tasks APIs
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/cluster-tasks-processor-context-test.xml",
		"classpath*:/SpringIOC/**/*.xml"
})
public class ClusterTasksProcessorCountTasksITCase extends CTSTestsBase {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorCountTasksITCase.class);

	@Autowired
	private ClusterTasksProcessorCount_test clusterTasksProcessorCount_test;

	@Test
	public void TestC_zero_results_all() {
		drainOutOldTasks();
		//assertEquals(0, clusterTasksProcessorCount_test.countTasks());
	}

	@Test
	public void TestD_zero_results_concurrency_given() {
		//assertEquals(0, clusterTasksProcessorCount_test.countTasksByConcurrencyKey("someNonExistingKey"));
	}

	@Test
	public void TestE_with_tasks() {
		drainOutOldTasks();

		ClusterTask[] tasks;
		String concurrencyKeyA = UUID.randomUUID().toString();
		String concurrencyKeyB = UUID.randomUUID().toString();

		//  create 6 tasks
		//      1 without concurrency key
		tasks = new ClusterTask[6];
		tasks[0] = new ClusterTask();

		//      2 with concurrency key A
		tasks[1] = new ClusterTask();
		tasks[1].setConcurrencyKey(concurrencyKeyA);
		tasks[2] = new ClusterTask();
		tasks[2].setConcurrencyKey(concurrencyKeyA);

		//      3 with concurrency key B
		tasks[3] = new ClusterTask();
		tasks[3].setConcurrencyKey(concurrencyKeyB);
		tasks[4] = new ClusterTask();
		tasks[4].setConcurrencyKey(concurrencyKeyB);
		tasks[5] = new ClusterTask();
		tasks[5].setConcurrencyKey(concurrencyKeyB);

		ClusterTaskPersistenceResult[] results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorCount_test", tasks);
		Arrays.stream(results).forEach(result ->
				assertEquals(CTPPersistStatus.SUCCESS, result.status)
		);

//		assertEquals(6, clusterTasksProcessorCount_test.countTasks());
//		assertEquals(1, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(null));
//		assertEquals(2, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(concurrencyKeyA));
//		assertEquals(3, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(concurrencyKeyB));
//
//		assertEquals(6, clusterTasksProcessorCount_test.countTasks(ClusterTaskStatus.PENDING));
//		assertEquals(1, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(null, ClusterTaskStatus.PENDING));
//		assertEquals(2, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(concurrencyKeyA, ClusterTaskStatus.PENDING));
//		assertEquals(3, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(concurrencyKeyB, ClusterTaskStatus.PENDING));
//
//		assertEquals(6, clusterTasksProcessorCount_test.countTasks(ClusterTaskStatus.PENDING, ClusterTaskStatus.RUNNING));
//		assertEquals(1, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(null, ClusterTaskStatus.PENDING, ClusterTaskStatus.RUNNING));
//		assertEquals(2, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(concurrencyKeyA, ClusterTaskStatus.PENDING, ClusterTaskStatus.RUNNING));
//		assertEquals(3, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(concurrencyKeyB, ClusterTaskStatus.PENDING, ClusterTaskStatus.RUNNING));
	}

	@Test
	public void TestF_count_tasks_with_concurr_key() {
		drainOutOldTasks();

		ClusterTask[] tasks;
		String concurrencyKey = UUID.randomUUID().toString();

		//  create 3 tasks with concurrency key
		tasks = new ClusterTask[3];
		tasks[0] = new ClusterTask();
		tasks[0].setConcurrencyKey(concurrencyKey);
		tasks[1] = new ClusterTask();
		tasks[1].setConcurrencyKey(concurrencyKey);
		tasks[2] = new ClusterTask();
		tasks[2].setConcurrencyKey(concurrencyKey);

		ClusterTaskPersistenceResult[] results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorCount_test", tasks);
		Arrays.stream(results).forEach(result ->
				assertEquals(CTPPersistStatus.SUCCESS, result.status)
		);

		//  hold tasks and check
//		assertEquals(3, clusterTasksProcessorCount_test.countTasks());
//		assertEquals(3, clusterTasksProcessorCount_test.countTasks(ClusterTaskStatus.PENDING));
//
//		assertEquals(3, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(concurrencyKey));
//		assertEquals(3, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(concurrencyKey, ClusterTaskStatus.PENDING));
//		assertEquals(0, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(null));

		//  release tasks (1 will be taken because of concurrency key) and check
		clusterTasksProcessorCount_test.holdTaskForMillis = 5000;
		clusterTasksProcessorCount_test.readyToTakeTasks = true;
		ClusterTasksITUtils.sleepSafely(2000);

//		assertEquals(1, clusterTasksProcessorCount_test.countTasks(ClusterTaskStatus.RUNNING));
//		assertEquals(2, clusterTasksProcessorCount_test.countTasks(ClusterTaskStatus.PENDING));
//		assertEquals(3, clusterTasksProcessorCount_test.countTasks(ClusterTaskStatus.RUNNING, ClusterTaskStatus.PENDING));
//
//		assertEquals(1, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(concurrencyKey, ClusterTaskStatus.RUNNING));
//		assertEquals(2, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(concurrencyKey, ClusterTaskStatus.PENDING));
//		assertEquals(3, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(concurrencyKey, ClusterTaskStatus.RUNNING, ClusterTaskStatus.PENDING));
	}

	@Test
	public void TestF_count_tasks_without_concurr_key() {
		drainOutOldTasks();

		ClusterTask[] tasks;

		//  create 3 tasks without concurrency key
		tasks = new ClusterTask[3];
		tasks[0] = new ClusterTask();
		tasks[1] = new ClusterTask();
		tasks[2] = new ClusterTask();

		ClusterTaskPersistenceResult[] results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorCount_test", tasks);
		Arrays.stream(results).forEach(result ->
				assertEquals(CTPPersistStatus.SUCCESS, result.status)
		);

		//  prevent tasks from running and count
//		assertEquals(3, clusterTasksProcessorCount_test.countTasks());
//		assertEquals(3, clusterTasksProcessorCount_test.countTasks(ClusterTaskStatus.PENDING));
//
//		assertEquals(3, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(null));
//		assertEquals(3, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(null, ClusterTaskStatus.PENDING));
//		assertEquals(0, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(null, ClusterTaskStatus.RUNNING));

		//  release tasks (2 will be taken because of 2 workers) and check
		clusterTasksProcessorCount_test.holdTaskForMillis = 7000;
		clusterTasksProcessorCount_test.readyToTakeTasks = true;
		ClusterTasksITUtils.sleepSafely(4000);

//		assertEquals(2, clusterTasksProcessorCount_test.countTasks(ClusterTaskStatus.RUNNING));
//		assertEquals(1, clusterTasksProcessorCount_test.countTasks(ClusterTaskStatus.PENDING));
//		assertEquals(3, clusterTasksProcessorCount_test.countTasks(ClusterTaskStatus.RUNNING, ClusterTaskStatus.PENDING));
//
//		assertEquals(2, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(null, ClusterTaskStatus.RUNNING));
//		assertEquals(1, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(null, ClusterTaskStatus.PENDING));
//		assertEquals(3, clusterTasksProcessorCount_test.countTasksByConcurrencyKey(null, ClusterTaskStatus.RUNNING, ClusterTaskStatus.PENDING));
	}

	private void drainOutOldTasks() {
		int maxRoundsToWaitDrain = 20;
		int waitDrainRounds = 0;
		clusterTasksProcessorCount_test.holdTaskForMillis = 0;
		clusterTasksProcessorCount_test.readyToTakeTasks = true;

//		while (waitDrainRounds++ < maxRoundsToWaitDrain && clusterTasksProcessorCount_test.countTasks() > 0) {
//			logger.info("draining out tasks, wait round " + waitDrainRounds);
//			ClusterTasksITUtils.sleepSafely(1000);
//		}

//		assertEquals("test's preliminary condition is 0 tasks in queue", 0, clusterTasksProcessorCount_test.countTasks());
		clusterTasksProcessorCount_test.readyToTakeTasks = false;
	}
}
