package com.microfocus.cluster.tasks.applicationkey;

import com.microfocus.cluster.tasks.CTSTestsBase;
import com.microfocus.cluster.tasks.CTSTestsUtils;
import com.microfocus.cluster.tasks.api.builders.TaskBuilders;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	private static final Logger logger = LoggerFactory.getLogger(ApplicationKeyTest.class);

	@Test
	public void testA_simple_app_key_chain() {
		AppKeyProcessorA_test.conditionToRun = "run";
		CTSTestsUtils.waitUntil(10000, () -> clusterTasksService.countTasksByApplicationKey(ClusterTasksDataProviderType.DB, "run", null) == 0 ? true : null);

		AppKeyProcessorA_test.tasksProcessed.clear();
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.isEmpty());

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
		clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "AppKeyProcessorA_test", task1, task2, task3);

		AppKeyProcessorA_test.conditionToRun = appKey;
		CTSTestsUtils.waitUntil(4000, () -> AppKeyProcessorA_test.tasksProcessed.size() == 3 ? true : null);

		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.containsKey("1"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.containsKey("2"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.containsKey("3"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.get("2") > AppKeyProcessorA_test.tasksProcessed.get("1"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.get("3") > AppKeyProcessorA_test.tasksProcessed.get("2"));
	}

	@Test
	public void testB_simple_app_key_chain_with_other_tasks() {
		AppKeyProcessorA_test.tasksProcessed.clear();
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.isEmpty());
		AppKeyProcessorA_test.conditionToRun = null;

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
		clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "AppKeyProcessorA_test", task1, task2, task3, task4, task5, task6);

		CTSTestsUtils.waitUntil(4000, () -> AppKeyProcessorA_test.tasksProcessed.size() == 3 ? true : null);

		AppKeyProcessorA_test.conditionToRun = appKey;
		CTSTestsUtils.waitUntil(6000, () -> AppKeyProcessorA_test.tasksProcessed.size() == 6 ? true : null);

		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.containsKey("1"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.containsKey("2"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.containsKey("3"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.containsKey("4"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.containsKey("5"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.containsKey("6"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.get("2") > AppKeyProcessorA_test.tasksProcessed.get("1"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.get("3") > AppKeyProcessorA_test.tasksProcessed.get("2"));
		//  ensure that held tasks ran after the non-held
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.get("1") > AppKeyProcessorA_test.tasksProcessed.get("4"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.get("1") > AppKeyProcessorA_test.tasksProcessed.get("5"));
		Assert.assertTrue(AppKeyProcessorA_test.tasksProcessed.get("1") > AppKeyProcessorA_test.tasksProcessed.get("6"));
	}
}

