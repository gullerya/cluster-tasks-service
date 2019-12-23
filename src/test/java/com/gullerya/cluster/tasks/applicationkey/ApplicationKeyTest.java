package com.gullerya.cluster.tasks.applicationkey;

import com.gullerya.cluster.tasks.api.builders.TaskBuilders;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.CTSTestsBase;
import com.gullerya.cluster.tasks.CTSTestsUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.UUID;

/**
 * Created by gullery on 22/05/2019.
 * <p>
 * Main collection of integration tests for Cluster Tasks Processor - Application Key related APIs (behaviour and count)
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/application-key-tests-context.xml"
})
public class ApplicationKeyTest extends CTSTestsBase {

	@Test
	public void testASimpleAppKeyChain() {
		AppKeyProcessorATest.any = true;
		CTSTestsUtils.waitSafely(5000);

		AppKeyProcessorATest.any = false;
		AppKeyProcessorATest.conditionToRun = null;
		AppKeyProcessorATest.tasksProcessed.clear();
		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.isEmpty());

		String appKey = UUID.randomUUID().toString();
		ClusterTask task1 = TaskBuilders.simpleTask()
				.setApplicationKey(appKey)
				.setBody("1")
				.build();
		ClusterTask task2 = TaskBuilders.simpleTask()
				.setApplicationKey(appKey)
				.setBody("2")
				.build();
		ClusterTask task3 = TaskBuilders.simpleTask()
				.setApplicationKey(appKey)
				.setBody("3")
				.build();
		clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "AppKeyProcessorATest", task1, task2, task3);

		CTSTestsUtils.waitSafely(4000);
		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.isEmpty());

		AppKeyProcessorATest.conditionToRun = appKey;
		CTSTestsUtils.waitUntil(4000, () -> AppKeyProcessorATest.tasksProcessed.size() == 3 ? true : null);

		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.containsKey("1"));
		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.containsKey("2"));
		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.containsKey("3"));
		Assert.assertTrue("expected positive diff, but got: " + (AppKeyProcessorATest.tasksProcessed.get("2") - AppKeyProcessorATest.tasksProcessed.get("1")), AppKeyProcessorATest.tasksProcessed.get("2") > AppKeyProcessorATest.tasksProcessed.get("1"));
		Assert.assertTrue("expected positive diff, but got: " + (AppKeyProcessorATest.tasksProcessed.get("3") - AppKeyProcessorATest.tasksProcessed.get("2")), AppKeyProcessorATest.tasksProcessed.get("3") > AppKeyProcessorATest.tasksProcessed.get("2"));
	}

	@Test
	public void testBSimpleAppKeyChainWithOtherTasks() {
		AppKeyProcessorATest.tasksProcessed.clear();
		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.isEmpty());
		AppKeyProcessorATest.conditionToRun = null;

		String appKey = UUID.randomUUID().toString();
		//  tasks first to be inserted, but with application key to hold on
		ClusterTask task1 = TaskBuilders.simpleTask()
				.setApplicationKey(appKey)
				.setBody("1")
				.build();
		ClusterTask task2 = TaskBuilders.simpleTask()
				.setApplicationKey(appKey)
				.setBody("2")
				.build();
		ClusterTask task3 = TaskBuilders.simpleTask()
				.setApplicationKey(appKey)
				.setBody("3")
				.build();
		//  tasks after the first ones, but without application key
		ClusterTask task4 = TaskBuilders.simpleTask()
				.setBody("4")
				.build();
		ClusterTask task5 = TaskBuilders.simpleTask()
				.setBody("5")
				.build();
		ClusterTask task6 = TaskBuilders.simpleTask()
				.setBody("6")
				.build();
		clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "AppKeyProcessorATest", task1, task2, task3, task4, task5, task6);

		CTSTestsUtils.waitUntil(4000, () -> AppKeyProcessorATest.tasksProcessed.size() == 3 ? true : null);

		AppKeyProcessorATest.conditionToRun = appKey;
		CTSTestsUtils.waitUntil(6000, () -> AppKeyProcessorATest.tasksProcessed.size() == 6 ? true : null);

		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.containsKey("1"));
		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.containsKey("2"));
		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.containsKey("3"));
		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.containsKey("4"));
		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.containsKey("5"));
		Assert.assertTrue(AppKeyProcessorATest.tasksProcessed.containsKey("6"));
		Assert.assertTrue("expected positive diff, but got: " + (AppKeyProcessorATest.tasksProcessed.get("2") - AppKeyProcessorATest.tasksProcessed.get("1")), AppKeyProcessorATest.tasksProcessed.get("2") > AppKeyProcessorATest.tasksProcessed.get("1"));
		Assert.assertTrue("expected positive diff, but got: " + (AppKeyProcessorATest.tasksProcessed.get("3") - AppKeyProcessorATest.tasksProcessed.get("2")), AppKeyProcessorATest.tasksProcessed.get("3") > AppKeyProcessorATest.tasksProcessed.get("2"));
		//  ensure that held tasks ran after the non-held
		Assert.assertTrue("expected positive diff, but got: " + (AppKeyProcessorATest.tasksProcessed.get("1") - AppKeyProcessorATest.tasksProcessed.get("4")), AppKeyProcessorATest.tasksProcessed.get("1") > AppKeyProcessorATest.tasksProcessed.get("4"));
		Assert.assertTrue("expected positive diff, but got: " + (AppKeyProcessorATest.tasksProcessed.get("1") - AppKeyProcessorATest.tasksProcessed.get("5")), AppKeyProcessorATest.tasksProcessed.get("1") > AppKeyProcessorATest.tasksProcessed.get("5"));
		Assert.assertTrue("expected positive diff, but got: " + (AppKeyProcessorATest.tasksProcessed.get("1") - AppKeyProcessorATest.tasksProcessed.get("6")), AppKeyProcessorATest.tasksProcessed.get("1") > AppKeyProcessorATest.tasksProcessed.get("6"));
	}

	@Test
	public void testCChanneledAppKeyChain() {
		AppKeyProcessorBTest.any = true;
		CTSTestsUtils.waitSafely(5000);

		AppKeyProcessorBTest.any = false;
		AppKeyProcessorBTest.tasksProcessed.clear();
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.isEmpty());
		AppKeyProcessorBTest.conditionToRun = null;

		String concurrencyKey = UUID.randomUUID().toString().replaceAll("-", "");
		String appKey = UUID.randomUUID().toString();
		//  tasks first to be inserted, all from the same channel, first one with application key to hold on
		ClusterTask task1 = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKey)
				.setApplicationKey(appKey)
				.setBody("1")
				.build();
		ClusterTask task2 = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKey)
				.setApplicationKey(appKey)
				.setBody("2")
				.build();
		ClusterTask task3 = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKey)
				.setApplicationKey(appKey)
				.setBody("3")
				.build();
		clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "AppKeyProcessorBTest", task1, task2, task3);

		CTSTestsUtils.waitSafely(4000);
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.isEmpty());

		AppKeyProcessorBTest.conditionToRun = appKey;
		CTSTestsUtils.waitUntil(4000, () -> AppKeyProcessorBTest.tasksProcessed.size() == 3 ? true : null);

		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.containsKey("1"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.containsKey("2"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.containsKey("3"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.get("2") > AppKeyProcessorBTest.tasksProcessed.get("1"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.get("3") > AppKeyProcessorBTest.tasksProcessed.get("2"));
	}

	@Test
	public void testDChanneledAppKeyChainWithOtherTasks() {
		AppKeyProcessorBTest.any = true;
		CTSTestsUtils.waitSafely(5000);

		AppKeyProcessorBTest.any = false;
		AppKeyProcessorBTest.tasksProcessed.clear();
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.isEmpty());
		AppKeyProcessorBTest.conditionToRun = null;

		String concurrencyKey = UUID.randomUUID().toString().replaceAll("-", "");
		String appKey = UUID.randomUUID().toString();
		//  tasks first to be inserted, all from the same channel, first one with application key to hold on
		ClusterTask task1 = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKey)
				.setApplicationKey(appKey)
				.setBody("1")
				.build();
		ClusterTask task2 = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKey)
				.setApplicationKey(appKey)
				.setBody("2")
				.build();
		ClusterTask task3 = TaskBuilders.channeledTask()
				.setConcurrencyKey(concurrencyKey)
				.setApplicationKey(appKey)
				.setBody("3")
				.build();
		//  tasks after the first ones, but without application key
		ClusterTask task4 = TaskBuilders.simpleTask()
				.setBody("4")
				.build();
		ClusterTask task5 = TaskBuilders.simpleTask()
				.setBody("5")
				.build();
		ClusterTask task6 = TaskBuilders.simpleTask()
				.setBody("6")
				.build();
		clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "AppKeyProcessorBTest", task1, task2, task3, task4, task5, task6);

		CTSTestsUtils.waitUntil(5000, () -> AppKeyProcessorBTest.tasksProcessed.size() == 3 ? true : null);

		AppKeyProcessorBTest.conditionToRun = appKey;
		CTSTestsUtils.waitUntil(6000, () -> AppKeyProcessorBTest.tasksProcessed.size() == 6 ? true : null);

		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.containsKey("1"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.containsKey("2"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.containsKey("3"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.containsKey("4"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.containsKey("5"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.containsKey("6"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.get("2") > AppKeyProcessorBTest.tasksProcessed.get("1"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.get("3") > AppKeyProcessorBTest.tasksProcessed.get("2"));
		//  ensure that held tasks ran after the non-held
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.get("1") > AppKeyProcessorBTest.tasksProcessed.get("4"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.get("1") > AppKeyProcessorBTest.tasksProcessed.get("5"));
		Assert.assertTrue(AppKeyProcessorBTest.tasksProcessed.get("1") > AppKeyProcessorBTest.tasksProcessed.get("6"));
	}
}

