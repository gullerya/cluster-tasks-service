package com.gullerya.cluster.tasks.impl;

import com.gullerya.cluster.tasks.api.builders.TaskBuilders;
import com.gullerya.cluster.tasks.api.dto.ClusterTask;

abstract public class TaskBuilderBase implements TaskBuilders.TaskBuilder {
	static int MAX_APPLICATION_KEY_LENGTH = 64;

	private volatile boolean locked;
	private ClusterTaskImpl result = new ClusterTaskImpl();

	protected TaskBuilderBase() {
	}

	protected TaskBuilders.TaskBuilder setUniquenessKeyInternal(String uniquenessKey) {
		if (locked) {
			throw new IllegalStateException("task builder MAY BE used only once");
		}
		if (uniquenessKey == null || uniquenessKey.isEmpty()) {
			throw new IllegalArgumentException("uniqueness key MUST NOT be null nor empty");
		}
		if (uniquenessKey.length() > 34) {
			throw new IllegalArgumentException("uniqueness key's length MUST BE less than or equal to 34 chars");
		}
		result.uniquenessKey = uniquenessKey;
		return this;
	}

	protected TaskBuilders.TaskBuilder setConcurrencyKeyInternal(String concurrencyKey, boolean untouched) throws IllegalStateException, IllegalArgumentException {
		if (locked) {
			throw new IllegalStateException("task builder MAY BE used only once");
		}
		if (concurrencyKey == null || concurrencyKey.isEmpty()) {
			throw new IllegalArgumentException("concurrency key MUST NOT be null nor empty");
		}
		if (concurrencyKey.length() > 34) {
			throw new IllegalArgumentException("concurrency key's length MUST BE less than or equal to 34 chars");
		}
		result.concurrencyKey = concurrencyKey;
		result.concurrencyKeyUntouched = untouched;
		return this;
	}

	public TaskBuilders.TaskBuilder setApplicationKey(String applicationKey) {
		if (locked) {
			throw new IllegalStateException("task builder MAY BE used only once");
		}
		if (applicationKey == null || applicationKey.isEmpty()) {
			throw new IllegalArgumentException("application key, if/when set, MUST NOT be NULL nor EMPTY");
		}
		if (applicationKey.length() > MAX_APPLICATION_KEY_LENGTH) {
			throw new IllegalArgumentException("application key MAY NOT exceed " + MAX_APPLICATION_KEY_LENGTH + " characters length");
		}
		result.applicationKey = applicationKey;
		return this;
	}

	public TaskBuilders.TaskBuilder setDelayByMillis(long delayByMillis) {
		if (locked) {
			throw new IllegalStateException("task builder MAY BE used only once");
		}
		if (delayByMillis < 0) {
			throw new IllegalArgumentException("delay MUST NOT be negative number");
		}
		result.delayByMillis = delayByMillis;
		return this;
	}

	public TaskBuilders.TaskBuilder setBody(String body) {
		if (locked) {
			throw new IllegalStateException("task builder MAY BE used only once");
		}
		if (body == null || body.isEmpty()) {
			throw new IllegalArgumentException("body, if/when set, MUST NOT be NULL nor EMPTY");
		}
		result.body = body;
		return this;
	}

	public ClusterTask build() {
		if (locked) {
			throw new IllegalStateException("task builder MAY BE built only once");
		}
		locked = true;
		return result;
	}
}
