package com.gullerya.cluster.tasks;

import com.gullerya.cluster.tasks.processors.scheduled.ClusterTasksSchedProcATest;
import com.gullerya.cluster.tasks.processors.scheduled.ClusterTasksSchedProcBTest;
import com.gullerya.cluster.tasks.processors.scheduled.ClusterTasksSchedProcCTest;
import com.gullerya.cluster.tasks.processors.scheduled.ClusterTasksSchedProcDTest;
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
	private ClusterTasksSchedProcDTest clusterTasksSchedProcD_test;

	@Test
	public void testAScheduledTasks() {
		//  resume processors
		ClusterTasksSchedProcATest.suspended = false;      // this CTP's self duration is ~1000 ms
		ClusterTasksSchedProcBTest.suspended = false;      // this CTP's self duration is ~2000 ms
		ClusterTasksSchedProcCTest.suspended = false;      // this CTP's self duration is ~3000 ms

		CTSTestsUtils.waitSafely(7000);

		//  suspend processors
		ClusterTasksSchedProcATest.suspended = true;
		ClusterTasksSchedProcBTest.suspended = true;
		ClusterTasksSchedProcCTest.suspended = true;

		//  verify executions
		logger.info("A - " + ClusterTasksSchedProcATest.executionsCounter + ", B - " + ClusterTasksSchedProcBTest.executionsCounter + ", C - " + ClusterTasksSchedProcCTest.executionsCounter);
		assertTrue(ClusterTasksSchedProcATest.executionsCounter >= 3 && ClusterTasksSchedProcATest.executionsCounter <= 6);
		assertTrue(ClusterTasksSchedProcBTest.executionsCounter >= 2 && ClusterTasksSchedProcBTest.executionsCounter <= 3);
		assertTrue(ClusterTasksSchedProcCTest.executionsCounter >= 1 && ClusterTasksSchedProcCTest.executionsCounter <= 2);
	}

	@Test
	public void testBReschedulingScheduledTaskWhenPending() {
		ClusterTasksSchedProcDTest.runAndHold = false;
		ClusterTasksSchedProcDTest.suspended = true;
		clusterTasksSchedProcD_test.reschedule(5000);
		ClusterTasksSchedProcDTest.executionsCounter = 0;
		ClusterTasksSchedProcDTest.suspended = false;
		CTSTestsUtils.waitSafely(7000);
		assertEquals(1, ClusterTasksSchedProcDTest.executionsCounter);

		clusterTasksSchedProcD_test.reschedule(0);
	}

	@Test
	public void testBReschedulingScheduledTaskWhenRunning() {
		ClusterTasksSchedProcDTest.executionsCounter = 0;
		ClusterTasksSchedProcDTest.suspended = false;
		ClusterTasksSchedProcDTest.runAndHold = true;
		CTSTestsUtils.waitUntil(3000, () -> {
			if (ClusterTasksSchedProcDTest.executionsCounter > 0) {
				return true;
			} else {
				return null;
			}
		});

		clusterTasksSchedProcD_test.reschedule(5000);
		ClusterTasksSchedProcDTest.executionsCounter = 0;
		ClusterTasksSchedProcDTest.runAndHold = false;
		CTSTestsUtils.waitSafely(7000);
		assertEquals(1, ClusterTasksSchedProcDTest.executionsCounter);

		clusterTasksSchedProcD_test.reschedule(0);
	}
}
