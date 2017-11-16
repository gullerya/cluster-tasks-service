package com.microfocus.octane.cluster.tasks.api;

import com.microfocus.octane.cluster.tasks.impl.ClusterTaskType;

/**
 * Created by gullery on 26/05/2016.
 * <p>
 * DTO bearing the task's information
 */

public class ClusterTask {
	public final Long id;
	public final ClusterTaskType taskType;
	public final String processorType;
	public final String uniquenessKey;
	public final String concurrencyKey;
	public final Long orderingFactor;
	public final Long delayByMillis;
	public final Long maxTimeToRunMillis;
	public final String body;

	ClusterTask(Long id,
	            ClusterTaskType taskType,
	            String processorType,
	            String uniquenessKey,
	            String concurrencyKey,
	            Long orderingFactor,
	            Long delayByMillis,
	            Long maxTimeToRunMillis,
	            String body) {
		this.id = id;
		this.taskType = taskType;
		this.processorType = processorType;
		this.uniquenessKey = uniquenessKey;
		this.concurrencyKey = concurrencyKey;
		this.orderingFactor = orderingFactor;
		this.delayByMillis = delayByMillis;
		this.maxTimeToRunMillis = maxTimeToRunMillis;
		this.body = body;
	}

	@Override
	public String toString() {
		return "ClusterTask {" +
				"id: " + id +
				", taskType: " + taskType +
				", processorType: " + processorType +
				", uniquenessKey: " + uniquenessKey +
				", concurrencyKey: " + concurrencyKey +
				", orderingFactor: " + orderingFactor +
				", delayByMillis: " + delayByMillis +
				", maxTimeToRunMillis: " + maxTimeToRunMillis + "}";
	}
}
