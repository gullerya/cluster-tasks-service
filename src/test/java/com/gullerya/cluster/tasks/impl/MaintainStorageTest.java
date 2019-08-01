package com.gullerya.cluster.tasks.impl;

import com.gullerya.cluster.tasks.CTSTestsUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by gullery on 31/07/2019.
 * <p>
 * Test/s to perform maintain storage task
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/maintain-storage-context-test.xml"
})
public class MaintainStorageTest {

	@Autowired
	private ClusterTasksServiceImpl clusterTasksService;

	@Test
	public void testMaintainStorage() throws Exception {
		Boolean ready = clusterTasksService.getReadyPromise().get();
		Assert.assertTrue(ready);

		CTSTestsUtils.waitSafely(1000);
		ClusterTasksMaintainer maintainer = clusterTasksService.getMaintainer();
		maintainer.maintainStorage(true);
	}
}
