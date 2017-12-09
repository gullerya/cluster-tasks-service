package com.microfocus.octane.cluster.tasks.api.builders;

import com.microfocus.octane.cluster.tasks.api.dto.ClusterTask;

public interface TaskBuilder {
	TaskBuilder setDelayByMillis(Long delayByMillis) throws IllegalStateException;

	TaskBuilder setMaxTimeToRunMillis(Long maxTimeToRunMillis) throws IllegalStateException;

	TaskBuilder setBody(String body) throws IllegalStateException;

	ClusterTask build() throws IllegalStateException;
}
