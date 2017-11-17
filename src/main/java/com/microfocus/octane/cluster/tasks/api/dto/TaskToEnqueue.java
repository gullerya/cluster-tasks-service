package com.microfocus.octane.cluster.tasks.api.dto;

import com.microfocus.octane.cluster.tasks.api.enums.ClusterTaskType;

/**
 * Created by gullery on 26/05/2016.
 * <p>
 * DTO bearing the task's information at the time of submission to for enqueue
 */

public class TaskToEnqueue {
	private String uniquenessKey;
	private String concurrencyKey;
	private Long orderingFactor;
	private Long delayByMillis;
	private Long maxTimeToRunMillis;
	private String body;

	private ClusterTaskType taskType = ClusterTaskType.REGULAR;

	public String getUniquenessKey() {
		return uniquenessKey;
	}

	public void setUniquenessKey(String uniquenessKey) {
		this.uniquenessKey = uniquenessKey;
	}

	public String getConcurrencyKey() {
		return concurrencyKey;
	}

	public void setConcurrencyKey(String concurrencyKey) {
		this.concurrencyKey = concurrencyKey;
	}

	public Long getOrderingFactor() {
		return orderingFactor;
	}

	public void setOrderingFactor(Long orderingFactor) {
		this.orderingFactor = orderingFactor;
	}

	public Long getDelayByMillis() {
		return delayByMillis;
	}

	public void setDelayByMillis(Long delayByMillis) {
		this.delayByMillis = delayByMillis;
	}

	public Long getMaxTimeToRunMillis() {
		return maxTimeToRunMillis;
	}

	public void setMaxTimeToRunMillis(Long maxTimeToRunMillis) {
		this.maxTimeToRunMillis = maxTimeToRunMillis;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public ClusterTaskType getTaskType() {
		return taskType;
	}

	public void setTaskType(ClusterTaskType taskType) {
		this.taskType = taskType == null ? ClusterTaskType.REGULAR : taskType;
	}

	@Override
	public String toString() {
		return "TaskToEnqueue {" +
				", uniquenessKey: " + uniquenessKey +
				", concurrencyKey: " + concurrencyKey +
				", orderingFactor: " + orderingFactor +
				", delayByMillis: " + delayByMillis +
				", maxTimeToRunMillis: " + maxTimeToRunMillis +
				", bodyLength: " + (body != null && !body.isEmpty() ? body.length() : "null") +
				", taskType: " + taskType +
				"}";
	}
}
