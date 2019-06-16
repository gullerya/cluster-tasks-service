package com.gullerya.cluster.tasks.impl;

import com.gullerya.cluster.tasks.api.enums.ClusterTaskType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by gullery on 02/06/2016.
 * <p>
 * Main collection of integration tests for Cluster Tasks Service's Utils
 */

public class ClusterTaskImplTest {

	@Test
	public void testA() {
		ClusterTaskImpl task = new ClusterTaskImpl();
		task.taskType = ClusterTaskType.REGULAR;
		task.processorType = "proc";
		task.uniquenessKey = "uniq";
		task.concurrencyKey = "con";
		task.applicationKey = "app";
		task.orderingFactor = 1000L;
		task.delayByMillis = 10000L;
		task.body = "body";
		task.partitionIndex = 0L;

		String stringify = task.toString();
		Assert.assertEquals(
				"ClusterTaskImpl {id: null, taskType: REGULAR, processorType: proc, uniquenessKey: uniq, concurrencyKey: con, applicationKey: app, orderingFactor: 1000, delayByMillis: 10000, bodyLength: 4, partitionIndex: 0}",
				stringify);
	}

	@Test
	public void testB() {
		ClusterTaskImpl task = new ClusterTaskImpl();
		task.taskType = ClusterTaskType.REGULAR;
		task.processorType = "proc";
		task.uniquenessKey = "uniq";
		task.concurrencyKey = "con";
		task.applicationKey = "app";
		task.orderingFactor = 1000L;
		task.delayByMillis = 10000L;
		task.partitionIndex = 0L;
		String stringifyTask = task.toString();

		ClusterTaskImpl next = new ClusterTaskImpl(task);
		String stringifyNext = next.toString();

		Assert.assertEquals(stringifyTask, stringifyNext);
	}

	@Test
	public void testC() {
		ClusterTaskImpl task = new ClusterTaskImpl();
		task.taskType = ClusterTaskType.REGULAR;
		task.processorType = "proc";
		task.uniquenessKey = "uniq";
		task.concurrencyKey = "con";
		task.applicationKey = "app";
		task.orderingFactor = 1000L;
		task.delayByMillis = 10000L;
		task.body = "";
		task.partitionIndex = 0L;
		String stringifyTask = task.toString();

		ClusterTaskImpl next = new ClusterTaskImpl(task);
		String stringifyNext = next.toString();

		Assert.assertEquals(stringifyTask, stringifyNext);
	}
}
