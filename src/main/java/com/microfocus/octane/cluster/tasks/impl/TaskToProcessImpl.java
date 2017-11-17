package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.dto.TaskToProcess;

/**
 * Created by gullery on 26/05/2016.
 * <p>
 * DTO bearing the task's information when handed over to processor
 */

class TaskToProcessImpl implements TaskToProcess {
	private Long id;
	private String uniquenessKey;
	private String concurrencyKey;
	private Long orderingFactor;
	private Long delayByMillis;
	private Long maxTimeToRunMillis;
	private String body;

	private TaskToProcessImpl() {
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
	public Long getOrderingFactor() {
		return orderingFactor;
	}

	@Override
	public Long getDelayByMillis() {
		return delayByMillis;
	}

	@Override
	public Long getMaxTimeToRunMillis() {
		return maxTimeToRunMillis;
	}

	@Override
	public String getBody() {
		return body;
	}

	static TaskToProcess from(TaskInternal origin) {
		TaskToProcessImpl result = new TaskToProcessImpl();
		result.id = origin.id;
		result.uniquenessKey = origin.uniquenessKey;
		result.concurrencyKey = origin.concurrencyKey;
		result.orderingFactor = origin.orderingFactor;
		result.delayByMillis = origin.delayByMillis;
		result.maxTimeToRunMillis = origin.maxTimeToRunMillis;
		result.body = origin.body;
		return result;
	}

	@Override
	public String toString() {
		return "TaskToProcess {" +
				", id: " + id +
				", uniquenessKey: " + uniquenessKey +
				", concurrencyKey: " + concurrencyKey +
				", orderingFactor: " + orderingFactor +
				", delayByMillis: " + delayByMillis +
				", maxTimeToRunMillis: " + maxTimeToRunMillis +
				", bodyLength: " + (body != null && !body.isEmpty() ? body.length() : "null") +
				"}";
	}
}
