package com.microfocus.octane.cluster.tasks.api.builders;

public interface ChanneledTaskBuilder {
	TaskBuilder setConcurrencyKey(String concurrencyKey) throws IllegalStateException, IllegalArgumentException;
}
