package com.microfocus.octane.cluster.tasks.impl;

/**
 * Created by gullery on 26/05/2016.
 * <p>
 * DTO bearing the task's information
 */

public class ClusterTaskInternal {
	private final Long id;
	private ClusterTaskType taskType = ClusterTaskType.REGULAR;
	private String processorType;
	private String uniquenessKey;
	private String concurrencyKey;
	private Long orderingFactor;
	private Long delayByMillis;
	private Long maxTimeToRunMillis;
	private String body;
	private Long partitionIndex;

	ClusterTaskInternal() {
		this.id = null;
	}

	ClusterTaskInternal(Long id) {
		this.id = id;
	}

	ClusterTaskInternal(ClusterTaskInternal origin) {
		if (origin == null) {
			throw new IllegalArgumentException("origin task MUST NOT be null");
		}

		id = origin.id;
		taskType = origin.taskType;
		processorType = origin.processorType;
		uniquenessKey = origin.uniquenessKey;
		concurrencyKey = origin.concurrencyKey;
		orderingFactor = origin.orderingFactor;
		delayByMillis = origin.delayByMillis;
		maxTimeToRunMillis = origin.maxTimeToRunMillis;
		body = origin.body;
		partitionIndex = origin.partitionIndex;
	}

	public Long getId() {
		return id;
	}

	public ClusterTaskType getTaskType() {
		return taskType;
	}

	public void setTaskType(ClusterTaskType taskType) {
		this.taskType = taskType;
	}

	public String getProcessorType() {
		return processorType;
	}

	public void setProcessorType(String processorType) {
		this.processorType = processorType;
	}

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

	public Long getPartitionIndex() {
		return partitionIndex;
	}

	public void setPartitionIndex(Long partitionIndex) {
		this.partitionIndex = partitionIndex;
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
				", maxTimeToRunMillis: " + maxTimeToRunMillis +
				", partitionIndex: " + partitionIndex + "}";
	}
}
