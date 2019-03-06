package com.microfocus.cluster.tasks;

import com.microfocus.cluster.tasks.api.ClusterTasksService;
import com.microfocus.cluster.tasks.processors.scheduled.ClusterTasksSchedProcA_test;
import com.microfocus.cluster.tasks.processors.scheduled.ClusterTasksSchedProcB_test;
import com.microfocus.cluster.tasks.processors.scheduled.ClusterTasksSchedProcC_test;
import com.microfocus.cluster.tasks.processors.scheduled.ClusterTasksSchedProcD_test;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
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
public class ScheduledProcessorTest extends CTSTestsBase {
	private static final Logger logger = LoggerFactory.getLogger(ScheduledProcessorTest.class);

	@Autowired
	private ClusterTasksService clusterTasksService;
	@Autowired
	private ClusterTasksSchedProcD_test clusterTasksSchedProcD_test;

	@Test
	public void testA_scheduled_tasks() {
		//  resume processors
		ClusterTasksSchedProcA_test.suspended = false;      // this CTP's self duration is ~1000 ms
		ClusterTasksSchedProcB_test.suspended = false;      // this CTP's self duration is ~2000 ms
		ClusterTasksSchedProcC_test.suspended = false;      // this CTP's self duration is ~3000 ms

		CTSTestsUtils.waitSafely(7000);

		//  suspend processors
		ClusterTasksSchedProcA_test.suspended = true;
		ClusterTasksSchedProcB_test.suspended = true;
		ClusterTasksSchedProcC_test.suspended = true;

		//  verify executions
		logger.info("A - " + ClusterTasksSchedProcA_test.executionsCounter + ", B - " + ClusterTasksSchedProcB_test.executionsCounter + ", C - " + ClusterTasksSchedProcC_test.executionsCounter);
		assertTrue(ClusterTasksSchedProcA_test.executionsCounter >= 3 && ClusterTasksSchedProcA_test.executionsCounter <= 6);
		assertTrue(ClusterTasksSchedProcB_test.executionsCounter >= 2 && ClusterTasksSchedProcB_test.executionsCounter <= 3);
		assertTrue(ClusterTasksSchedProcC_test.executionsCounter >= 1 && ClusterTasksSchedProcC_test.executionsCounter <= 2);
	}

	@Test
	public void testB_rescheduling_scheduled_task_when_pending() {
		ClusterTasksSchedProcD_test.runAndHold = false;
		ClusterTasksSchedProcD_test.suspended = true;
		clusterTasksSchedProcD_test.reschedule(5000);
		ClusterTasksSchedProcD_test.executionsCounter = 0;
		ClusterTasksSchedProcD_test.suspended = false;
		CTSTestsUtils.waitSafely(7000);
		assertEquals(1, ClusterTasksSchedProcD_test.executionsCounter);

		clusterTasksSchedProcD_test.reschedule(0);
	}

	@Test
	public void testB_rescheduling_scheduled_task_when_running() {
		ClusterTasksSchedProcD_test.executionsCounter = 0;
		ClusterTasksSchedProcD_test.suspended = false;
		ClusterTasksSchedProcD_test.runAndHold = true;
		CTSTestsUtils.waitUntil(3000, () -> {
			if (ClusterTasksSchedProcD_test.executionsCounter > 0) {
				return true;
			} else {
				return null;
			}
		});

		clusterTasksSchedProcD_test.reschedule(5000);
		ClusterTasksSchedProcD_test.executionsCounter = 0;
		ClusterTasksSchedProcD_test.runAndHold = false;
		CTSTestsUtils.waitSafely(7000);
		assertEquals(1, ClusterTasksSchedProcD_test.executionsCounter);

		clusterTasksSchedProcD_test.reschedule(0);
	}

	@Test
	public void testC_attempt_to_enqueue_scheduled_task() {
		//  TODO: write test that tries to enqueue scheduled task bypassing the normal mechanism (should fail)
		//  TODO: the test should do that in 2 phases, once when the scheduled task in pending state and once when the task is running
	}
}
