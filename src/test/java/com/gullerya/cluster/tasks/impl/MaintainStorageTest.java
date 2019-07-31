/*
	(c) Copyright 2018 Micro Focus or one of its affiliates.
	Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
	You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
	Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and limitations under the License.
 */

package com.gullerya.cluster.tasks.impl;

import com.gullerya.cluster.tasks.CTSTestsUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
	private static final Logger logger = LoggerFactory.getLogger(MaintainStorageTest.class);

	@Autowired
	private ClusterTasksServiceImpl clusterTasksService;

	@Test
	public void testMaintainStorage() throws Exception {
		Boolean ready = clusterTasksService.getReadyPromise().get();
		if (ready) {
			CTSTestsUtils.waitSafely(1000);
			ClusterTasksMaintainer maintainer = clusterTasksService.getMaintainer();
			maintainer.maintainStorage(true);
		} else  {
			throw new IllegalStateException("CTS failed to initialize");
		}
	}
}
