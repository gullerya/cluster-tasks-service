package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskType;

/**
 * Created by gullery on 26/05/2016.
 * <p>
 * DTO bearing the task's information
 */

class TaskInternal {
	Long id;
	ClusterTaskType taskType = ClusterTaskType.REGULAR;
	String processorType;
	String uniquenessKey;
	String concurrencyKey;
	Long orderingFactor;
	Long delayByMillis;
	Long maxTimeToRunMillis;
	String body;
	Long partitionIndex;

	TaskInternal() {
	}

	TaskInternal(TaskInternal source) {
		if (source == null) {
			throw new IllegalArgumentException("source MUST NOT be null");
		}
		id = source.id;
		taskType = source.taskType;
		processorType = source.processorType;
		uniquenessKey = source.uniquenessKey;
		concurrencyKey = source.concurrencyKey;
		orderingFactor = source.orderingFactor;
		delayByMillis = source.delayByMillis;
		maxTimeToRunMillis = source.maxTimeToRunMillis;
		body = source.body;
		partitionIndex = source.partitionIndex;
	}

	@Override
	public String toString() {
		return "TaskInternal {" +
				"id: " + id +
				", taskType: " + taskType +
				", processorType: " + processorType +
				", uniquenessKey: " + uniquenessKey +
				", concurrencyKey: " + concurrencyKey +
				", orderingFactor: " + orderingFactor +
				", delayByMillis: " + delayByMillis +
				", maxTimeToRunMillis: " + maxTimeToRunMillis +
				", bodyLength: " + (body != null && !body.isEmpty() ? body.length() : "null") +
				", partitionIndex: " + partitionIndex + "}";
	}
}
