package com.gullerya.cluster.tasks.applicationkey;

import com.gullerya.cluster.tasks.api.builders.TaskBuilders;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;
import com.gullerya.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.gullerya.cluster.tasks.api.enums.ClusterTaskInsertStatus;
import com.gullerya.cluster.tasks.api.enums.ClusterTaskStatus;
import com.gullerya.cluster.tasks.api.enums.ClusterTasksDataProviderType;
import com.gullerya.cluster.tasks.CTSTestsBase;
import com.gullerya.cluster.tasks.CTSTestsUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.UUID;

/**
 * Created by gullery on 22/05/2019.
 * <p>
 * Main collection of integration tests for Cluster Tasks Processor - Application Key related APIs (behaviour and count)
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/application-key-count-tests-context.xml"
})
public class ApplicationKeyCountTest extends CTSTestsBase {

	@Test(expected = IllegalArgumentException.class)
	public void negA() {
		clusterTasksService.countTasksByApplicationKey(null, null, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negB() {
		clusterTasksService.countTasksByApplicationKey(ClusterTasksDataProviderType.DB, String.join("", Collections.nCopies(3, UUID.randomUUID().toString())), null);
	}

	@Test
	public void testASimpleAppKeyCount() {
		AppKeyProcessorCount_test.any = true;
		CTSTestsUtils.waitSafely(3000);
		AppKeyProcessorCount_test.any = false;
		AppKeyProcessorCount_test.conditionToRun = null;
		AppKeyProcessorCount_test.tasksProcessed.clear();

		String appKey = UUID.randomUUID().toString();
		ClusterTask[] tasks = new ClusterTask[3];
		tasks[0] = TaskBuilders.simpleTask()
				.setApplicationKey(appKey)
				.build();
		tasks[1] = TaskBuilders.simpleTask()
				.setApplicationKey(appKey)
				.build();
		tasks[2] = TaskBuilders.simpleTask()
				.setApplicationKey(appKey)
				.build();

		ClusterTaskPersistenceResult[] pr = clusterTasksService.enqueueTasks(ClusterTasksDataProviderType.DB, "AppKeyProcessorCount_test", tasks);
		for (ClusterTaskPersistenceResult r : pr) {
			Assert.assertEquals(ClusterTaskInsertStatus.SUCCESS, r.getStatus());
		}

		//  assert pending count is correct
		int count = clusterTasksService.countTasksByApplicationKey(ClusterTasksDataProviderType.DB, appKey, ClusterTaskStatus.PENDING);
		Assert.assertEquals(tasks.length, count);

		//  release tasks to run and assert running count is correct
		AppKeyProcessorCount_test.holdRunning = true;
		AppKeyProcessorCount_test.conditionToRun = appKey;
		CTSTestsUtils.waitSafely(2500);
		count = clusterTasksService.countTasksByApplicationKey(ClusterTasksDataProviderType.DB, appKey, ClusterTaskStatus.RUNNING);
		Assert.assertEquals(tasks.length, count);

		AppKeyProcessorCount_test.holdRunning = false;

		//  wait to finalize run and assert no tasks anymore
		CTSTestsUtils.waitSafely(1000);
		count = clusterTasksService.countTasksByApplicationKey(ClusterTasksDataProviderType.DB, appKey, ClusterTaskStatus.PENDING);
		Assert.assertEquals(0, count);
		count = clusterTasksService.countTasksByApplicationKey(ClusterTasksDataProviderType.DB, appKey, ClusterTaskStatus.RUNNING);
		Assert.assertEquals(0, count);

		Assert.assertEquals(tasks.length, AppKeyProcessorCount_test.tasksProcessed.size());
	}
}

