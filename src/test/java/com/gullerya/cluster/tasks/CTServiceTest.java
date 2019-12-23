package com.gullerya.cluster.tasks;

import com.gullerya.cluster.tasks.api.builders.TaskBuilders;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.gullerya.cluster.tasks.api.enums.ClusterTaskInsertStatus;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.processors.*;
import com.gullerya.cluster.tasks.processors.ClusterTasksProcessorFTestCNA;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Main collection of integration tests for Cluster Tasks Processor Service
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/cluster-tasks-service-context-test.xml"
})
public class CTServiceTest extends CTSTestsBase {
	private final Logger logger = LoggerFactory.getLogger(CTServiceTest.class);

	@Autowired
	private ClusterTasksProcessorATest clusterTasksProcessorA_test;
	@Autowired
	private ClusterTasksProcessorBTest clusterTasksProcessorB_test;
	@Autowired
	private ClusterTasksProcessorCTest clusterTasksProcessorC_test;
	@Autowired
	private ClusterTasksProcessorDTest clusterTasksProcessorD_test;
	@Autowired
	private ClusterTasksProcessorETestNA clusterTasksProcessorE_testNA;
	@Autowired
	private ClusterTasksProcessorFTestCNA clusterTasksProcessorF_testCna;

	@Test
	public void testASingleProcessor() {
		clusterTasksProcessorA_test.tasksProcessed.clear();

		int tasksNumber = 5;
		String concurrencyKey = "testA";
		List<ClusterTask> tasks = new LinkedList<>();
		ClusterTask tmp;

		for (int i = 0; i < tasksNumber; i++) {
			tmp = TaskBuilders.channeledTask()
					.setConcurrencyKey(concurrencyKey)
					.setBody(String.valueOf(i))
					.build();
			tasks.add(tmp);
		}

		ClusterTaskPersistenceResult[] result = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorATest", tasks.toArray(new ClusterTask[0]));
		for (ClusterTaskPersistenceResult r : result) {
			assertEquals(ClusterTaskInsertStatus.SUCCESS, r.getStatus());
		}

		waitResultsContainerComplete(clusterTasksProcessorA_test.tasksProcessed, tasksNumber, 1000 * 4 * tasksNumber);

		assertEquals(tasksNumber, clusterTasksProcessorA_test.tasksProcessed.size());

		for (int i = 0; i < tasksNumber; i++) {
			assertEquals(String.valueOf(i), new ArrayList<>(clusterTasksProcessorA_test.tasksProcessed.keySet()).get(i));
		}

		assertTrue(clusterTasksProcessorA_test.tasksProcessed.get("1").after(clusterTasksProcessorA_test.tasksProcessed.get("0")));
		assertTrue(clusterTasksProcessorA_test.tasksProcessed.get("2").after(clusterTasksProcessorA_test.tasksProcessed.get("1")));
	}

	@Test
	public void testBTwoProcessorsSingleConcurrencyKey() {
		clusterTasksProcessorB_test.tasksProcessed.clear();
		clusterTasksProcessorC_test.tasksProcessed.clear();

		String concurrencyKey = "testB";
		List<ClusterTask> tasks = new LinkedList<>();
		List<ClusterTaskPersistenceResult> results = new LinkedList<>();
		ClusterTask tmp;

		for (int i = 0; i < 5; i++) {
			tmp = TaskBuilders.channeledTask()
					.setConcurrencyKey(concurrencyKey, true)
					.setBody(String.valueOf(i))
					.build();
			tasks.add(tmp);
		}

		results.add(clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorBTest", tasks.get(0))[0]);
		results.add(clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorCTest", tasks.get(1))[0]);
		results.add(clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorBTest", tasks.get(2))[0]);
		results.add(clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorCTest", tasks.get(3))[0]);
		results.add(clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorBTest", tasks.get(4))[0]);
		for (ClusterTaskPersistenceResult r : results) {
			assertEquals(ClusterTaskInsertStatus.SUCCESS, r.getStatus());
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
	public void testCProcessorCustomDispatchInterval() {
		List<ClusterTask> tasks = new LinkedList<>();
		ClusterTask tmp;
		String concurrencyKey = UUID.randomUUID().toString().replaceAll("-", "");
		ClusterTaskPersistenceResult[] results;

		clusterTasksProcessorD_test.tasksProcessed.clear();

		//  2 tasks for customized interval behavior check
		tmp = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKey)
				.setBody("nonsense1")
				.build();
		tasks.add(tmp);
		tmp = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKey)
				.setBody("nonsense2")
				.build();
		tasks.add(tmp);
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorDTest", tasks.toArray(new ClusterTask[0]));
		for (ClusterTaskPersistenceResult r : results) {
			assertEquals(ClusterTaskInsertStatus.SUCCESS, r.getStatus());
		}
		waitResultsContainerComplete(clusterTasksProcessorD_test.tasksProcessed, 2, 10000);
		assertEquals(2, clusterTasksProcessorD_test.tasksProcessed.size());
		assertTrue(clusterTasksProcessorD_test.tasksProcessed.get("nonsense2").getTime() - clusterTasksProcessorD_test.tasksProcessed.get("nonsense1").getTime() > 7000);
	}

	@Test
	public void testDTasksWithUniquenessKeys() {
		List<ClusterTask> tasks = new LinkedList<>();
		ClusterTask tmp;
		ClusterTaskPersistenceResult[] results;
		String uniqueKey = UUID.randomUUID().toString().replaceAll("-", "");
		clusterTasksProcessorA_test.tasksProcessed.clear();

		//  task 1 with the same unique key
		tmp = TaskBuilders.uniqueTask().setUniquenessKey(uniqueKey).setBody("something").build();
		tasks.add(tmp);

		//  task 2 with the same unique key
		tmp = TaskBuilders.uniqueTask().setUniquenessKey(uniqueKey).setBody("something").build();
		tasks.add(tmp);

		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "NoneExistingTaskProcessor", tasks.toArray(new ClusterTask[0]));
		assertEquals(ClusterTaskInsertStatus.SUCCESS, results[0].getStatus());
		assertEquals(ClusterTaskInsertStatus.UNIQUE_CONSTRAINT_FAILURE, results[1].getStatus());
	}

	@Test
	public void testEDelayedTasks() {
		ClusterTask tmp;
		String concurrencyKey = UUID.randomUUID().toString().replaceAll("-", "");
		long delay = 7000L;
		ClusterTaskPersistenceResult[] results;
		clusterTasksProcessorA_test.tasksProcessed.clear();

		//  task 1 with the same concurrency key
		tmp = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKey)
				.setDelayByMillis(delay)
				.setBody("delayed")
				.build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorATest", tmp);
		assertEquals(ClusterTaskInsertStatus.SUCCESS, results[0].getStatus());

		//  task 2 with the same concurrency key
		tmp = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKey)
				.setBody("first_to_run")
				.build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorATest", tmp);
		assertEquals(ClusterTaskInsertStatus.SUCCESS, results[0].getStatus());

		long startWait = System.currentTimeMillis();
		Long passedTime = CTSTestsUtils.waitUntil(10000, () -> {
			if (clusterTasksProcessorA_test.tasksProcessed.containsKey("first_to_run") && clusterTasksProcessorA_test.tasksProcessed.containsKey("delayed")) {
				return System.currentTimeMillis() - startWait;
			} else {
				return null;
			}
		});
		assertNotNull(passedTime);

		logger.info("delay: " + delay + "; passed: " + passedTime);
		//  precision of seconds is enough, since we are storing the time data as date and not timestamp
		//  and it is possible that delay would be fulfilled withing up to 1 second less
		assertTrue("passed:  " + passedTime + " should be bigger than delay: " + delay, passedTime >= delay);
		assertEquals("first_to_run", new ArrayList<>(clusterTasksProcessorA_test.tasksProcessed.keySet()).get(0));
		assertEquals("delayed", new ArrayList<>(clusterTasksProcessorA_test.tasksProcessed.keySet()).get(1));
		assertTrue(clusterTasksProcessorA_test.tasksProcessed.get("delayed").after(clusterTasksProcessorA_test.tasksProcessed.get("first_to_run")));
	}

	@Test
	public void testFNonAvailableTaskHoldingConcurrencyKey() {
		ClusterTask tmp;
		ClusterTaskPersistenceResult[] results;
		String concurrencyKey = UUID.randomUUID().toString().replaceAll("-", "");
		String taskBodyToCheck = "visited here";

		//  enqueue first task to an ever-non-available processor
		tmp = TaskBuilders.channeledTask().setConcurrencyKey(concurrencyKey).build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorETestNA", tmp);
		assertEquals(ClusterTaskInsertStatus.SUCCESS, results[0].getStatus());

		//  enqueue second task to an available processor with the same concurrency key
		tmp = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKey)
				.setBody(taskBodyToCheck)
				.build();
		results = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "ClusterTasksProcessorFTestCNA", tmp);
		assertEquals(ClusterTaskInsertStatus.SUCCESS, results[0].getStatus());

		//  ensure that the 'staled' non-available processor's task in NOT holding the whole concurrent queue
		waitResultsContainerComplete(clusterTasksProcessorF_testCna.tasksProcessed, 1, 3000);
		assertEquals(taskBodyToCheck, clusterTasksProcessorF_testCna.tasksProcessed.get(taskBodyToCheck));
	}

	private <K, V> long waitResultsContainerComplete(Map<K, V> container, int expectedSize, long maxTimeToWait) {
		long timePassed = 0;
		long pauseInterval = 50;
		while (container.size() != expectedSize && timePassed < maxTimeToWait) {
			CTSTestsUtils.waitSafely(pauseInterval);
			timePassed += pauseInterval;
		}
		if (container.size() == expectedSize) {
			logger.info("expectation fulfilled in " + timePassed + "ms");
		} else {
			fail("expectation " + expectedSize + " was not fulfilled (found " + container.size() + ") in given " + maxTimeToWait + "ms");
		}
		return timePassed;
	}
}
