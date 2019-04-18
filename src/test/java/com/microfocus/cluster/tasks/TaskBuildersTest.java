package com.microfocus.cluster.tasks;

import com.microfocus.cluster.tasks.api.builders.TaskBuilders;
import com.microfocus.cluster.tasks.api.dto.ClusterTask;
import com.microfocus.cluster.tasks.api.enums.ClusterTaskType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;

/**
 * Created by gullery on 18/04/2019.
 * <p>
 * Main collection of integration tests for Cluster Tasks Processor - Task Builders API
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({
		"/cluster-tasks-service-context-test.xml"
})
public class TaskBuildersTest extends CTSTestsBase {
	private static final Logger logger = LoggerFactory.getLogger(TaskBuildersTest.class);

	//	channelled tasks

	@Test(expected = IllegalArgumentException.class)
	public void testChA() {
		TaskBuilders.channeledTask()
				.setConcurrencyKey(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testChB() {
		TaskBuilders.channeledTask()
				.setConcurrencyKey("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testChC() {
		TaskBuilders.channeledTask()
				.setConcurrencyKey(String.join("", Collections.nCopies(35, "0")));
	}

	@Test(expected = IllegalStateException.class)
	public void testChD() {
		TaskBuilders.ChanneledTaskBuilder chBuilder = TaskBuilders.channeledTask();
		chBuilder.setConcurrencyKey("something");
		chBuilder.build();
		chBuilder.setConcurrencyKey("some");
	}

	@Test
	public void testChE() {
		ClusterTask task = TaskBuilders.channeledTask()
				.setConcurrencyKey("some key")
				.setDelayByMillis(1000)
				.setBody("body")
				.build();

		Assert.assertNotNull(task);
		Assert.assertNull(task.getUniquenessKey());
		Assert.assertEquals("some key", task.getConcurrencyKey());
		Assert.assertEquals(1000, (long) task.getDelayByMillis());
		Assert.assertEquals("body", task.getBody());
	}

	//	unique tasks

	@Test(expected = IllegalArgumentException.class)
	public void testUnA() {
		TaskBuilders.uniqueTask()
				.setUniquenessKey(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnB() {
		TaskBuilders.uniqueTask()
				.setUniquenessKey("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnC() {
		TaskBuilders.uniqueTask()
				.setUniquenessKey(String.join("", Collections.nCopies(35, "0")));
	}

	@Test(expected = IllegalStateException.class)
	public void testUnD() {
		TaskBuilders.UniqueTaskBuilder chBuilder = TaskBuilders.uniqueTask();
		chBuilder.setUniquenessKey("something");
		chBuilder.build();
		chBuilder.setUniquenessKey("some");
	}

	@Test
	public void testUnE() {
		ClusterTask task = TaskBuilders.uniqueTask()
				.setUniquenessKey("some key")
				.setDelayByMillis(1000)
				.setBody("body")
				.build();

		Assert.assertNotNull(task);
		Assert.assertEquals("some key", task.getUniquenessKey());
		Assert.assertNull(task.getConcurrencyKey());
		Assert.assertEquals(1000, (long) task.getDelayByMillis());
		Assert.assertEquals("body", task.getBody());
	}

	//  simple tasks
	@Test(expected = IllegalArgumentException.class)
	public void testStA() {
		TaskBuilders.simpleTask()
				.setBody(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testStB() {
		TaskBuilders.simpleTask()
				.setBody("");
	}

	@Test(expected = IllegalStateException.class)
	public void testStC() {
		TaskBuilders.TaskBuilder taskBuilder = TaskBuilders.simpleTask();
		taskBuilder.setBody("something");
		taskBuilder.build();
		taskBuilder.setBody("some");
	}

	@Test(expected = IllegalStateException.class)
	public void testStD() {
		TaskBuilders.TaskBuilder taskBuilder = TaskBuilders.simpleTask();
		taskBuilder.setDelayByMillis(1000);
		taskBuilder.build();
		taskBuilder.setDelayByMillis(900);
	}

	//  enums

	@Test(expected = IllegalArgumentException.class)
	public void testEnumA() {
		ClusterTaskType.byValue(2);
	}
}

