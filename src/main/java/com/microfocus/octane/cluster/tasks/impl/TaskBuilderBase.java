package com.microfocus.octane.cluster.tasks.impl;

import com.microfocus.octane.cluster.tasks.api.builders.TaskBuilder;
import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;

abstract public class TaskBuilderBase implements TaskBuilder {
	protected volatile boolean locked = false;
	protected String uniquenessKey;
	protected String concurrencyKey;
	private Long delayByMillis;
	private Long maxTimeToRunMillis;
	private String body;

	protected TaskBuilderBase() {
	}

	public TaskBuilder setDelayByMillis(Long delayByMillis) {
		if (locked) throw new IllegalStateException("task builder MAY BE used only once");
		this.delayByMillis = delayByMillis;
		return this;
	}

	public TaskBuilder setMaxTimeToRunMillis(Long maxTimeToRunMillis) {
		if (locked) throw new IllegalStateException("task builder MAY BE used only once");
		this.maxTimeToRunMillis = maxTimeToRunMillis;
		return this;
	}

	public TaskBuilder setBody(String body) {
		if (locked) throw new IllegalStateException("task builder MAY BE used only once");
		this.body = body;
		return this;
	}

	public ClusterTask build() {
		if (locked) throw new IllegalStateException("task builder MAY BE used only once");
		locked = true;
		ClusterTaskImpl result = new ClusterTaskImpl();
		result.uniquenessKey = uniquenessKey;
		result.concurrencyKey = concurrencyKey;
		result.delayByMillis = delayByMillis;
		result.maxTimeToRunMillis = maxTimeToRunMillis;
		result.body = body;
		return result;
	}
}
