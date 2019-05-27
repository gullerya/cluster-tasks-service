package com.microfocus.cluster.tasks.applicationkey;

import com.microfocus.cluster.tasks.CTSTestsBase;
import com.microfocus.cluster.tasks.CTSTestsUtils;
import com.microfocus.cluster.tasks.api.builders.TaskBuilders;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.dto.ClusterTaskPersistenceResult;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskInsertStatus;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskStatus;
import com.microfocus.cluster.tasks.api.enums.ClusterTasksDataProviderType;
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
		"/application-key-count-tests-context.xml"
})
public class ApplicationKeyCountTest extends CTSTestsBase {

	@Test
	public void testA_simple_app_key_count() {
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
		AppKeyProcessorCount_test.conditionToRun = appKey;
		CTSTestsUtils.waitSafely(1500);
		count = clusterTasksService.countTasksByApplicationKey(ClusterTasksDataProviderType.DB, appKey, ClusterTaskStatus.RUNNING);
		Assert.assertEquals(tasks.length, count);

		//  wait to finalize run and assert no tasks anymore
		CTSTestsUtils.waitSafely(3000);
		count = clusterTasksService.countTasksByApplicationKey(ClusterTasksDataProviderType.DB, appKey, ClusterTaskStatus.PENDING);
		Assert.assertEquals(0, count);
		count = clusterTasksService.countTasksByApplicationKey(ClusterTasksDataProviderType.DB, appKey, ClusterTaskStatus.RUNNING);
		Assert.assertEquals(0, count);

		Assert.assertEquals(tasks.length, AppKeyProcessorCount_test.tasksProcessed.size());
	}
}

