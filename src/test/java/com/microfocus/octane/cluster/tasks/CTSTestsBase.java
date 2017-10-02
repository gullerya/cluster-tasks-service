package com.microfocus.octane.cluster.tasks;

import com.microfocus.octane.cluster.tasks.api.ClusterTasksService;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;

abstract class CTSTestsBase {
	@Autowired
	ClusterTasksService clusterTasksService;

	@Before
	public void ensurePrerequisites() throws InterruptedException {
		clusterTasksService.getReadyPromise().join();
	}
}
