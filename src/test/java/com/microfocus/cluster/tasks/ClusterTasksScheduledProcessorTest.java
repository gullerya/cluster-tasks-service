package com.microfocus.cluster.tasks;

import com.microfocus.cluster.tasks.processors.scheduled.ClusterTasksSchedProcA_test;
import com.microfocus.cluster.tasks.processors.scheduled.ClusterTasksSchedProcB_test;
import com.microfocus.cluster.tasks.processors.scheduled.ClusterTasksSchedProcC_test;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertTrue;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Collection of integration tests for Cluster Tasks Processor Service to check specifically the functionality of scheduled tasks
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/cluster-tasks-scheduled-processor-context-test.xml"
})
public class ClusterTasksScheduledProcessorTest extends CTSTestsBase {
	private static final Logger logger = LoggerFactory.getLogger(ClusterTasksScheduledProcessorTest.class);

	@Test
	public void TestA_scheduled_tasks() {
		//  resume processors
		ClusterTasksSchedProcA_test.suspended = false;      // this CTP's self duration is ~1000 ms
		ClusterTasksSchedProcB_test.suspended = false;      // this CTP's self duration is ~2000 ms
		ClusterTasksSchedProcC_test.suspended = false;      // this CTP's self duration is ~3000 ms

		ClusterTasksTestsUtils.sleepSafely(7000);

		//  suspend processors
		ClusterTasksSchedProcA_test.suspended = true;
		ClusterTasksSchedProcB_test.suspended = true;
		ClusterTasksSchedProcC_test.suspended = true;

		//  verify executions
		logger.info("A - " + ClusterTasksSchedProcA_test.tasksCounter + ", B - " + ClusterTasksSchedProcB_test.tasksCounter + ", C - " + ClusterTasksSchedProcC_test.tasksCounter);
		assertTrue(ClusterTasksSchedProcA_test.tasksCounter >= 3 && ClusterTasksSchedProcA_test.tasksCounter <= 6);
		assertTrue(ClusterTasksSchedProcB_test.tasksCounter >= 2 && ClusterTasksSchedProcB_test.tasksCounter <= 3);
		assertTrue(ClusterTasksSchedProcC_test.tasksCounter >= 1 && ClusterTasksSchedProcC_test.tasksCounter <= 2);
	}
}
