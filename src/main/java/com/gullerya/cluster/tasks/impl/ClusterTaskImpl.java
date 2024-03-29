package com.gullerya.cluster.tasks.impl;

import com.gullerya.cluster.tasks.api.enums.ClusterTaskType;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;

/**
 * Created by gullery on 26/05/2016.
 * <p>
 * DTO bearing the task's information and used in consumer's oriented flows:
 * - when the tasks are submitted for enqueue, immutable ClusterTask interface exposed to the consumer having this implementation under the hood
 * - when the tasks are handed over to the processor, same as above is happening
 */

class ClusterTaskImpl implements ClusterTask {
	Long id;
	ClusterTaskType taskType = ClusterTaskType.REGULAR;
	String processorType;
	String uniquenessKey;
	String concurrencyKey;
	boolean concurrencyKeyUntouched;
	String applicationKey;
	Long orderingFactor;
	Long delayByMillis;
	String body;
	Long partitionIndex;

	ClusterTaskImpl() {
	}

	ClusterTaskImpl(ClusterTaskImpl origin) {
		id = origin.id;
		taskType = origin.taskType;
		processorType = origin.processorType;
		uniquenessKey = origin.uniquenessKey;
		concurrencyKey = origin.concurrencyKey;
		concurrencyKeyUntouched = origin.concurrencyKeyUntouched;
		applicationKey = origin.applicationKey;
		orderingFactor = origin.orderingFactor;
		delayByMillis = origin.delayByMillis;
		body = origin.body;
		partitionIndex = origin.partitionIndex;
	}

	@Override
	public Long getId() {
		return id;
	}

	@Override
	public String getUniquenessKey() {
		return uniquenessKey;
	}

	@Override
	public String getConcurrencyKey() {
		return concurrencyKey;
	}

	@Override
	public String getApplicationKey() {
		return applicationKey;
	}

	@Override
	public Long getOrderingFactor() {
		return orderingFactor;
	}

	@Override
	public Long getDelayByMillis() {
		return delayByMillis;
	}

	@Override
	public String getBody() {
		return body;
	}

	@Override
	public String toString() {
		return "ClusterTaskImpl {" +
				"id: " + id +
				", taskType: " + taskType +
				", processorType: " + processorType +
				", uniquenessKey: " + uniquenessKey +
				", concurrencyKey: " + concurrencyKey +
				", applicationKey: " + applicationKey +
				", orderingFactor: " + orderingFactor +
				", delayByMillis: " + delayByMillis +
				", bodyLength: " + (body != null ? body.length() : "null") +
				", partitionIndex: " + partitionIndex + "}";
	}
}
