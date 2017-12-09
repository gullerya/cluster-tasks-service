package com.microfocus.octane.cluster.tasks.api.builders;

public interface UniqueTaskBuilder {
	TaskBuilder setUniquenessKey(String uniquenessKey) throws IllegalStateException, IllegalArgumentException;
}
