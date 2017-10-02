package com.microfocus.octane.cluster.tasks;

import com.microfocus.octane.cluster.tasks.api.CTPPersistStatus;
import com.microfocus.octane.cluster.tasks.api.ClusterTask;
import com.microfocus.octane.cluster.tasks.api.ClusterTaskPersistenceResult;
import com.microfocus.octane.cluster.tasks.api.ClusterTasksDataProviderType;
import com.microfocus.octane.cluster.tasks.processors.ClusterTasksProcessorA_test;
import com.microfocus.octane.cluster.tasks.processors.ClusterTasksProcessorB_test;
import com.microfocus.octane.cluster.tasks.processors.ClusterTasksProcessorC_test;
import com.microfocus.octane.cluster.tasks.processors.ClusterTasksProcessorD_test;
import com.microfocus.octane.cluster.tasks.processors.ClusterTasksProcessorE_test_na;
import com.microfocus.octane.cluster.tasks.processors.ClusterTasksProcessorF_test_cna;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Main collection of integration tests for Cluster Tasks Processor Service
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/cluster-tasks-processor-context-test.xml",
		"classpath*:/SpringIOC/**/*.xml"
})
public class ClusterTasksProcessorServiceTest extends CTSTestsBase {
	private final Logger logger = LoggerFactory.getLogger(ClusterTasksProcessorServiceTest.class);

	@Autowired
	private ClusterTasksProcessorA_test clusterTasksProcessorA_test;
	@Autowired
	private ClusterTasksProcessorB_test clusterTasksProcessorB_test;
	@Autowired
	private ClusterTasksProcessorC_test clusterTasksProcessorC_test;
	@Autowired
	private ClusterTasksProcessorD_test clusterTasksProcessorD_test;
	@Autowired
	private ClusterTasksProcessorE_test_na clusterTasksProcessorE_test_na;
	@Autowired
	private ClusterTasksProcessorF_test_cna clusterTasksProcessorF_test_cna;

	@Test
	public void TestA_single_processor() {
		clusterTasksProcessorA_test.tasksProcessed.clear();

		String concurrencyKey = "testA";
		List<ClusterTask> tasks = new LinkedList<>();
		ClusterTask tmp;

		for (int i = 0; i < 3; i++) {
			tmp = new ClusterTask();
			tmp.setConcurrencyKey(concurrencyKey);
			tmp.setBody(String.valueOf(i));
			tasks.add(tmp);
		}

		ClusterTaskPersistenceResult[] result = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorA_test", tasks.toArray(new ClusterTask[tasks.size()]));
		for (ClusterTaskPersistenceResult r : result) {
			assertEquals(CTPPersistStatus.SUCCESS, r.status);
		}

		waitResultsContainerComplete(clusterTasksProcessorA_test.tasksProcessed, 3, 1000 * 15);

		assertEquals(3, clusterTasksProcessorA_test.tasksProcessed.size());

		assertEquals("0", new ArrayList<>(clusterTasksProcessorA_test.tasksProcessed.keySet()).get(0));
		assertEquals("1", new ArrayList<>(clusterTasksProcessorA_test.tasksProcessed.keySet()).get(1));
		assertEquals("2", new ArrayList<>(clusterTasksProcessorA_test.tasksProcessed.keySet()).get(2));

		assertTrue(clusterTasksProcessorA_test.tasksProcessed.get("1").after(clusterTasksProcessorA_test.tasksProcessed.get("0")));
		assertTrue(clusterTasksProcessorA_test.tasksProcessed.get("2").after(clusterTasksProcessorA_test.tasksProcessed.get("1")));
	}

	@Test
	public void TestB_two_processors_single_concurrency_key() {
		clusterTasksProcessorB_test.tasksProcessed.clear();
		clusterTasksProcessorC_test.tasksProcessed.clear();

		String concurrencyKey = "testB";
		List<ClusterTask> tasks = new LinkedList<>();
		List<ClusterTaskPersistenceResult> results = new LinkedList<>();
		ClusterTask tmp;

		for (int i = 0; i < 5; i++) {
			tmp = new ClusterTask();
			tmp.setConcurrencyKey(concurrencyKey);
			tmp.setBody(String.valueOf(i));
			tasks.add(tmp);
		}

		results.add(clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorB_test", tasks.get(0))[0]);
		results.add(clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorC_test", tasks.get(1))[0]);
		results.add(clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorB_test", tasks.get(2))[0]);
		results.add(clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorC_test", tasks.get(3))[0]);
		results.add(clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorB_test", tasks.get(4))[0]);
		for (ClusterTaskPersistenceResult r : results) {
			assertEquals(CTPPersistStatus.SUCCESS, r.status);
		}

		waitResultsContainerComplete(clusterTasksProcessorB_test.tasksProcessed, 3, 1000 * 20);

		assertEquals(3, clusterTasksProcessorB_test.tasksProcessed.size());
		assertEquals(2, clusterTasksProcessorC_test.tasksProcessed.size());

		assertEquals("0", new ArrayList<>(clusterTasksProcessorB_test.tasksProcessed.keySet()).get(0));
		assertEquals("1", new ArrayList<>(clusterTasksProcessorC_test.tasksProcessed.keySet()).get(0));
		assertEquals("2", new ArrayList<>(clusterTasksProcessorB_test.tasksProcessed.keySet()).get(1));
		assertEquals("3", new ArrayList<>(clusterTasksProcessorC_test.tasksProcessed.keySet()).get(1));
		assertEquals("4", new ArrayList<>(clusterTasksProcessorB_test.tasksProcessed.keySet()).get(2));

		assertTrue(clusterTasksProcessorC_test.tasksProcessed.get("1").after(clusterTasksProcessorB_test.tasksProcessed.get("0")));
		assertTrue(clusterTasksProcessorB_test.tasksProcessed.get("2").after(clusterTasksProcessorC_test.tasksProcessed.get("1")));
		assertTrue(clusterTasksProcessorC_test.tasksProcessed.get("3").after(clusterTasksProcessorB_test.tasksProcessed.get("2")));
		assertTrue(clusterTasksProcessorB_test.tasksProcessed.get("4").after(clusterTasksProcessorC_test.tasksProcessed.get("3")));
	}

	@Test
	public void TestC_processor_custom_dispatch_interval() {
		List<ClusterTask> tasks = new LinkedList<>();
		ClusterTask tmp;
		String concurrencyKey = UUID.randomUUID().toString();
		ClusterTaskPersistenceResult[] results;
		long doneInInterval;

		clusterTasksProcessorD_test.tasksProcessed.clear();

		//  2 tasks for customized interval behavior check
		tmp = new ClusterTask();
		tmp.setConcurrencyKey(concurrencyKey);
		tmp.setBody("nonsense1");
		tasks.add(tmp);
		tmp = new ClusterTask();
		tmp.setConcurrencyKey(concurrencyKey);
		tmp.setBody("nonsense2");
		tasks.add(tmp);
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorD_test", tasks.toArray(new ClusterTask[tasks.size()]));
		for (ClusterTaskPersistenceResult r : results) {
			assertEquals(CTPPersistStatus.SUCCESS, r.status);
		}
		doneInInterval = waitResultsContainerComplete(clusterTasksProcessorD_test.tasksProcessed, 2, 15000);
		assertEquals(2, clusterTasksProcessorD_test.tasksProcessed.size());
		assertTrue(clusterTasksProcessorD_test.tasksProcessed.get("nonsense2").getTime() - clusterTasksProcessorD_test.tasksProcessed.get("nonsense1").getTime() > 7000);
	}

	@Test
	public void TestD_tasks_with_uniqueness_keys() {
		List<ClusterTask> tasks = new LinkedList<>();
		ClusterTask tmp;
		ClusterTaskPersistenceResult[] results;
		String uniqueKey = UUID.randomUUID().toString();
		clusterTasksProcessorA_test.tasksProcessed.clear();

		//  task 1 with the same unique key
		tmp = new ClusterTask();
		tmp.setUniquenessKey(uniqueKey);
		tmp.setConcurrencyKey(UUID.randomUUID().toString());
		tasks.add(tmp);

		//  task 2 with the same unique key
		tmp = new ClusterTask();
		tmp.setUniquenessKey(uniqueKey);
		tmp.setConcurrencyKey(UUID.randomUUID().toString());
		tasks.add(tmp);

		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorA_test", tasks.toArray(new ClusterTask[tasks.size()]));
		assertEquals(CTPPersistStatus.SUCCESS, results[0].status);
		assertEquals(CTPPersistStatus.UNIQUE_CONSTRAINT_FAILURE, results[1].status);
		waitResultsContainerComplete(clusterTasksProcessorA_test.tasksProcessed, 1, 3000);
	}

	@Test
	public void TestE_delayed_tasks() {
		ClusterTask tmp;
		String concurrencyKey = UUID.randomUUID().toString();
		long delay = 6000L;
		ClusterTaskPersistenceResult[] results;
		clusterTasksProcessorA_test.tasksProcessed.clear();

		//  task 1 with the same concurrency key
		tmp = new ClusterTask();
		tmp.setConcurrencyKey(concurrencyKey);
		tmp.setDelayByMillis(delay);
		tmp.setBody("delayed");
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorA_test", tmp);
		assertEquals(CTPPersistStatus.SUCCESS, results[0].status);

		//  task 2 with the same concurrency key
		tmp = new ClusterTask();
		tmp.setConcurrencyKey(concurrencyKey);
		tmp.setBody("first_to_run");
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorA_test", tmp);
		assertEquals(CTPPersistStatus.SUCCESS, results[0].status);

		long passedTime = waitResultsContainerComplete(clusterTasksProcessorA_test.tasksProcessed, 2, 10000);
		assertTrue(passedTime >= delay);
		assertEquals("first_to_run", new ArrayList<>(clusterTasksProcessorA_test.tasksProcessed.keySet()).get(0));
		assertEquals("delayed", new ArrayList<>(clusterTasksProcessorA_test.tasksProcessed.keySet()).get(1));
		assertTrue(clusterTasksProcessorA_test.tasksProcessed.get("delayed").after(clusterTasksProcessorA_test.tasksProcessed.get("first_to_run")));
	}

	@Test
	public void TestF_non_available_task_holding_concurrency_key() {
		ClusterTask tmp;
		ClusterTaskPersistenceResult[] results;
		String concurrencyKey = UUID.randomUUID().toString();
		String taskBodyToCheck = "visited here";

		//  enqueue first task to an ever-non-available processor
		tmp = new ClusterTask();
		tmp.setConcurrencyKey(concurrencyKey);
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorE_test_na", tmp);
		assertEquals(CTPPersistStatus.SUCCESS, results[0].status);

		//  enqueue second task to an available processor with the same concurrency key
		tmp = new ClusterTask();
		tmp.setConcurrencyKey(concurrencyKey);
		tmp.setBody(taskBodyToCheck);
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorF_test_cna", tmp);
		assertEquals(CTPPersistStatus.SUCCESS, results[0].status);

		//  ensure that the 'staled' non-available processor's task in NOT holding the whole concurrent queue
		waitResultsContainerComplete(clusterTasksProcessorF_test_cna.tasksProcessed, 1, 3000);
		assertEquals(taskBodyToCheck, clusterTasksProcessorF_test_cna.tasksProcessed.get(taskBodyToCheck));
	}

	private <K, V> long waitResultsContainerComplete(Map<K, V> container, int expectedSize, long maxTimeToWait) {
		long timePassed = 0;
		long pauseInterval = 437;
		while (container.size() != expectedSize && timePassed < maxTimeToWait) {
			ClusterTasksITUtils.sleepSafely(pauseInterval);
			timePassed += pauseInterval;
		}
		if (container.size() == expectedSize) {
			logger.info("expectation fulfilled in " + timePassed + "ms");
			System.out.println("expectation fulfilled in " + timePassed + "ms");
		} else {
			fail("expectation " + expectedSize + " was not fulfilled (found " + container.size() + ") in given " + maxTimeToWait + "ms");
		}
		return timePassed;
	}
}
